"""
主扫描调度引擎

数据源: RootData（DrissionPage CDP + DOM 提取）— 需登录
流程: RootData CDP 采集 → CMC 核对 → 入库
"""
import json
import logging
import os
import uuid
from datetime import datetime

from core import db
from core.cmc_verifier import CMCVerifier

logger = logging.getLogger("scanner")


class Scanner:
    """融资代币扫描调度器。"""

    def __init__(self, proxy: str | None = None, cmc_api_key: str = "",
                 rootdata_email: str = "", rootdata_password: str = ""):
        self.proxy = proxy
        self.cmc_api_key = cmc_api_key
        self._cmc = CMCVerifier(api_key=cmc_api_key, proxy=proxy) if cmc_api_key else None
        self._rootdata_email = rootdata_email or os.environ.get("ROOTDATA_EMAIL", "")
        self._rootdata_password = rootdata_password or os.environ.get("ROOTDATA_PASSWORD", "")

    def run_full_scan(self, on_log=None,
                      max_rootdata_pages: int = 10,
                      enable_cmc_verify: bool = True,
                      **kwargs) -> dict:
        """执行全量扫描。

        Returns:
            {scan_id, total_projects, funded_with_token, not_listed, cmc_verified}
        """
        scan_id = str(uuid.uuid4())[:8]
        conn = db.get_connection()

        try:
            db.create_scan_log(conn, scan_id, "full")

            if on_log:
                on_log(f"[Scanner] 扫描 {scan_id} 开始")

            # ── 1. RootData CDP 采集 ──
            projects = []
            if self._rootdata_email:
                if on_log:
                    on_log("=" * 40)
                    on_log("[Scanner] 阶段 ① RootData CDP 融资数据采集")
                try:
                    from core.rootdata_scraper import RootDataCDPScraper
                    rd = RootDataCDPScraper(
                        email=self._rootdata_email,
                        password=self._rootdata_password,
                    )
                    projects = rd.fetch_all_pages(
                        max_pages=max_rootdata_pages, on_log=on_log,
                    )
                    rd.close()
                    if on_log:
                        on_log(f"[Scanner] RootData 获取 {len(projects)} 个项目")
                except Exception as e:
                    logger.warning("[Scanner] RootData 采集失败: %s", e)
                    if on_log:
                        on_log(f"[Scanner] RootData 采集失败: {e}")
            else:
                if on_log:
                    on_log("[Scanner] RootData 未配置账号 (ROOTDATA_EMAIL)，跳过")

            if not projects:
                if on_log:
                    on_log("[Scanner] 无数据采集，扫描结束")
                db.update_scan_log(conn, scan_id, status="done",
                                   total_projects=0, finished_at=datetime.now().isoformat())
                return {"scan_id": scan_id, "status": "done", "total_projects": 0}

            # ── 2. 去重入库 ──
            if on_log:
                on_log("=" * 40)
                on_log("[Scanner] 阶段 ② 去重 + 入库")

            project_map: dict[str, dict] = {}
            for proj in projects:
                name = proj["project_name"].strip().lower()
                if name not in project_map:
                    project_map[name] = proj

            total_saved = 0
            tokens_saved = 0
            for proj in project_map.values():
                pid = db.upsert_project(conn, proj)
                total_saved += 1

                symbol = proj.get("token_symbol", "")
                if symbol:
                    token_data = {
                        "token_symbol": symbol,
                        "token_name": proj.get("project_name", ""),
                        "chain": proj.get("chain", ""),
                    }
                    db.upsert_token(conn, pid, token_data)
                    tokens_saved += 1

            if on_log:
                on_log(f"[Scanner] 入库 {total_saved} 个项目, {tokens_saved} 个代币")

            # ── 3. CMC 核对 ──
            cmc_verified = 0
            if enable_cmc_verify and self._cmc and tokens_saved > 0:
                if on_log:
                    on_log("=" * 40)
                    on_log("[Scanner] 阶段 ③ CMC API 上所状态核对")

                tokens = conn.execute(
                    "SELECT id, token_symbol FROM tokens WHERE token_symbol != ''"
                ).fetchall()

                for i, token_row in enumerate(tokens):
                    tid = token_row[0]
                    sym = token_row[1]
                    if on_log:
                        on_log(f"[CMC] [{i+1}/{len(tokens)}] 验证 {sym}")

                    result = self._cmc.verify_token(sym)
                    now = datetime.now().isoformat()

                    conn.execute("""
                        UPDATE tokens SET
                            cmc_listed = ?,
                            cmc_market_pairs = ?,
                            exchanges = ?,
                            last_verified_at = ?,
                            verification_source = 'cmc'
                        WHERE id = ?
                    """, (
                        1 if result["cmc_listed"] else 0,
                        result["num_market_pairs"],
                        json.dumps(result["exchanges"], ensure_ascii=False),
                        now, tid,
                    ))
                    conn.commit()
                    cmc_verified += 1

                if on_log:
                    on_log(f"[Scanner] CMC 核对完成: {cmc_verified} 个代币")

            # ── 4. 更新扫描日志 ──
            not_listed = conn.execute(
                "SELECT COUNT(*) FROM tokens WHERE cmc_listed = 0 "
                "AND token_symbol != ''"
            ).fetchone()[0]

            db.update_scan_log(conn, scan_id,
                               status="done",
                               total_projects=total_saved,
                               funded_with_token=tokens_saved,
                               not_listed=not_listed,
                               cmc_verified=cmc_verified,
                               finished_at=datetime.now().isoformat())

            result = {
                "scan_id": scan_id,
                "status": "done",
                "total_projects": total_saved,
                "funded_with_token": tokens_saved,
                "not_listed": not_listed,
                "cmc_verified": cmc_verified,
            }

            if on_log:
                on_log("=" * 40)
                on_log(f"[Scanner] 扫描完成: {json.dumps(result, ensure_ascii=False)}")

            return result

        except Exception as e:
            logger.exception("[Scanner] 扫描失败")
            db.update_scan_log(conn, scan_id,
                               status="error",
                               error_message=str(e),
                               finished_at=datetime.now().isoformat())
            if on_log:
                on_log(f"[Scanner] 扫描失败: {e}")
            return {"scan_id": scan_id, "status": "error", "error": str(e)}

        finally:
            conn.close()
