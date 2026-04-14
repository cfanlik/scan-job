"""
主扫描调度引擎

支持三种扫描模式:
  full        - 全量扫描（max_pages=0 自动读取总页数）
  incremental - 增量扫描（仅采集 DB 中不存在的新项目）
  auto        - DB 为空 → full；否则 → incremental
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
        self.proxy              = proxy
        self.cmc_api_key        = cmc_api_key
        self._cmc               = CMCVerifier(api_key=cmc_api_key, proxy=proxy) if cmc_api_key else None
        self._rootdata_email    = rootdata_email    or os.environ.get("ROOTDATA_EMAIL", "")
        self._rootdata_password = rootdata_password or os.environ.get("ROOTDATA_PASSWORD", "")

    # ────────────────────────────────────────
    #  内部：CMC 验证
    # ────────────────────────────────────────

    def _run_cmc_verify(self, conn, on_log=None) -> int:
        """对 DB 中有 symbol 的代币做 CMC 状态核对，返回核对数量。"""
        tokens = conn.execute(
            "SELECT id, token_symbol FROM tokens WHERE token_symbol != ''"
        ).fetchall()

        cmc_verified = 0
        for i, token_row in enumerate(tokens):
            tid = token_row[0]
            sym = token_row[1]
            if on_log:
                on_log(f"[CMC] [{i+1}/{len(tokens)}] 验证 {sym}")

            result = self._cmc.verify_token(sym)
            now    = datetime.now().isoformat()
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

        return cmc_verified

    # ────────────────────────────────────────
    #  内部：入库
    # ────────────────────────────────────────

    def _save_projects(self, conn, projects: list[dict], on_log=None) -> tuple[int, int]:
        """入库项目列表，返回 (total_saved, tokens_saved)。"""
        project_map: dict[str, dict] = {}
        for proj in projects:
            name = proj["project_name"].strip().lower()
            if name not in project_map:
                project_map[name] = proj

        total_saved  = 0
        tokens_saved = 0
        for proj in project_map.values():
            pid = db.upsert_project(conn, proj)
            total_saved += 1
            symbol = proj.get("token_symbol", "")
            if symbol:
                db.upsert_token(conn, pid, {
                    "token_symbol": symbol,
                    "token_name":   proj.get("project_name", ""),
                    "chain":        proj.get("chain", ""),
                })
                tokens_saved += 1

        if on_log:
            on_log(f"[Scanner] 入库 {total_saved} 个项目, {tokens_saved} 个代币")
        return total_saved, tokens_saved

    # ────────────────────────────────────────
    #  内部：代币发现
    # ────────────────────────────────────────

    def _run_token_discovery(self, conn, on_log=None) -> int:
        """对 DB 中无 token 的项目执行 CMC Map 本地匹配，返回新发现数量。"""
        from core.token_discovery import TokenDiscovery

        projects = db.get_projects_without_token(conn)
        if not projects:
            if on_log:
                on_log("[Discovery] 所有项目已有代币信息，跳过")
            return 0

        if on_log:
            on_log(f"[Discovery] {len(projects)} 个项目待匹配")

        discovery = TokenDiscovery(cmc_api_key=self.cmc_api_key, proxy=self.proxy)
        result = discovery.batch_discover(projects, on_log=on_log)

        matched_count = 0
        for m in result["matched"]:
            db.upsert_token(conn, m["id"], {
                "token_symbol": m["symbol"],
                "token_name":   m["cmc_name"],
                "cmc_listed":   1,
                "verification_source": f"cmc_map_{m['match_method']}",
            })
            matched_count += 1

        if on_log:
            on_log(f"[Discovery] 入库 {matched_count} 个代币")
        return matched_count

    # ────────────────────────────────────────
    #  全量扫描
    # ────────────────────────────────────────

    def run_full_scan(self, on_log=None,
                      max_rootdata_pages: int = 10,
                      enable_cmc_verify: bool = True,
                      **kwargs) -> dict:
        """全量扫描。max_rootdata_pages=0 自动读取总页数。"""
        scan_id = str(uuid.uuid4())[:8]
        conn    = db.get_connection()

        try:
            db.create_scan_log(conn, scan_id, "full")
            if on_log:
                on_log(f"[Scanner] 扫描 {scan_id} 开始 [全量]")

            # ── 1. RootData 采集 ──
            projects = []
            if self._rootdata_email:
                if on_log:
                    on_log("=" * 40)
                    on_log("[Scanner] 阶段 ① RootData 融资数据采集")
                try:
                    from core.rootdata_scraper import RootDataCDPScraper
                    rd = RootDataCDPScraper(
                        email=self._rootdata_email,
                        password=self._rootdata_password,
                    )
                    projects = rd.fetch_all_pages(
                        max_pages=max_rootdata_pages,
                        on_log=on_log,
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
                                   total_projects=0, new_projects=0,
                                   finished_at=datetime.now().isoformat())
                return {"scan_id": scan_id, "status": "done", "total_projects": 0, "new_projects": 0}

            # ── 2. 入库 ──
            if on_log:
                on_log("=" * 40)
                on_log("[Scanner] 阶段 ② 去重 + 入库")
            total_saved, tokens_saved = self._save_projects(conn, projects, on_log)

            # ── 2.5 代币发现（CMC Map 本地匹配）──
            discovery_matched = 0
            if self.cmc_api_key:
                if on_log:
                    on_log("=" * 40)
                    on_log("[Scanner] 阶段 ②.5 代币发现 (CMC Map 批量匹配)")
                discovery_matched = self._run_token_discovery(conn, on_log)
                tokens_saved += discovery_matched

            # ── 3. CMC 核对 ──
            cmc_verified = 0
            if enable_cmc_verify and self._cmc and tokens_saved > 0:
                if on_log:
                    on_log("=" * 40)
                    on_log("[Scanner] 阶段 ③ CMC API 上市状态核对")
                cmc_verified = self._run_cmc_verify(conn, on_log)
                if on_log:
                    on_log(f"[Scanner] CMC 核对完成: {cmc_verified} 个代币")

            # ── 4. 结果 ──
            not_listed = conn.execute(
                "SELECT COUNT(*) FROM tokens WHERE cmc_listed=0 AND token_symbol!=''"
            ).fetchone()[0]

            db.update_scan_log(conn, scan_id,
                               status="done",
                               total_projects=total_saved,
                               new_projects=total_saved,   # 全量首次均为新增
                               funded_with_token=tokens_saved,
                               not_listed=not_listed,
                               cmc_verified=cmc_verified,
                               finished_at=datetime.now().isoformat())

            result = {
                "scan_id": scan_id, "status": "done",
                "total_projects": total_saved, "new_projects": total_saved,
                "funded_with_token": tokens_saved,
                "not_listed": not_listed, "cmc_verified": cmc_verified,
            }
            if on_log:
                on_log("=" * 40)
                on_log(f"[Scanner] 扫描完成: {json.dumps(result, ensure_ascii=False)}")
            return result

        except Exception as e:
            logger.exception("[Scanner] 扫描失败")
            db.update_scan_log(conn, scan_id, status="error",
                               error_message=str(e),
                               finished_at=datetime.now().isoformat())
            if on_log:
                on_log(f"[Scanner] 扫描失败: {e}")
            return {"scan_id": scan_id, "status": "error", "error": str(e)}
        finally:
            conn.close()

    # ────────────────────────────────────────
    #  增量扫描
    # ────────────────────────────────────────

    def run_incremental_scan(self, on_log=None,
                             max_rootdata_pages: int = 50,
                             enable_cmc_verify: bool = True) -> dict:
        """增量扫描：仅采集 DB 中不存在的新项目。
        
        策略:
          - 加载 DB 中已有的 rootdata_url set
          - 每页回调 early_stop_fn：若该页已知数 ≥ 28 → 计数+1，连续2页 → 停止
          - 仅将新增项目入库
        """
        scan_id = str(uuid.uuid4())[:8]
        conn    = db.get_connection()

        try:
            db.create_scan_log(conn, scan_id, "incremental")
            if on_log:
                on_log(f"[Scanner] 扫描 {scan_id} 开始 [增量]")

            # 加载已有 URL 集合
            known_urls = db.get_known_rootdata_urls(conn)
            if on_log:
                on_log(f"[Scanner] DB 已有项目: {len(known_urls)} 条 (按 rootdata_url 去重)")

            if not self._rootdata_email:
                if on_log:
                    on_log("[Scanner] RootData 未配置账号 (ROOTDATA_EMAIL)，跳过")
                db.update_scan_log(conn, scan_id, status="done",
                                   total_projects=0, new_projects=0,
                                   finished_at=datetime.now().isoformat())
                return {"scan_id": scan_id, "status": "done", "new_projects": 0}

            # ── 1. RootData 增量采集 ──
            if on_log:
                on_log("=" * 40)
                on_log("[Scanner] 阶段 ① RootData 增量采集 (early stop 模式)")

            all_collected: list[dict] = []

            def early_stop_fn(page_projects: list[dict]) -> bool:
                """当页中已知项目 ≥ 28 条则触发 early stop。"""
                known_count = sum(
                    1 for p in page_projects
                    if p.get("rootdata_url") in known_urls
                )
                ratio = known_count / len(page_projects) if page_projects else 0
                if on_log:
                    on_log(
                        f"[Scanner] early stop 检测: {known_count}/{len(page_projects)} 已知 "
                        f"({ratio:.0%})"
                    )
                return known_count >= 28  # 28/30 = 93% 触发停止

            try:
                from core.rootdata_scraper import RootDataCDPScraper
                rd = RootDataCDPScraper(
                    email=self._rootdata_email,
                    password=self._rootdata_password,
                )
                all_collected = rd.fetch_all_pages(
                    max_pages=max_rootdata_pages,
                    on_log=on_log,
                    early_stop_fn=early_stop_fn,
                )
                rd.close()
            except Exception as e:
                logger.warning("[Scanner] RootData 增量采集失败: %s", e)
                if on_log:
                    on_log(f"[Scanner] RootData 采集失败: {e}")
                db.update_scan_log(conn, scan_id, status="error",
                                   error_message=str(e),
                                   finished_at=datetime.now().isoformat())
                return {"scan_id": scan_id, "status": "error", "error": str(e)}

            # ── 2. 过滤出真正的新增 ──
            new_projects = [
                p for p in all_collected
                if p.get("rootdata_url") not in known_urls
            ]
            if on_log:
                on_log("=" * 40)
                on_log(
                    f"[Scanner] 阶段 ② 筛选新增: 采集 {len(all_collected)} 条 → "
                    f"新增 {len(new_projects)} 条"
                )

            # ── 3. 入库 ──
            total_saved  = 0
            tokens_saved = 0
            if new_projects:
                total_saved, tokens_saved = self._save_projects(conn, new_projects, on_log)
            else:
                if on_log:
                    on_log("[Scanner] 无新增项目，跳过入库")

            # ── 3.5 代币发现（CMC Map 本地匹配）──
            discovery_matched = 0
            if self.cmc_api_key and total_saved > 0:
                if on_log:
                    on_log("=" * 40)
                    on_log("[Scanner] 阶段 ③.5 代币发现 (CMC Map 批量匹配)")
                discovery_matched = self._run_token_discovery(conn, on_log)
                tokens_saved += discovery_matched

            # ── 4. CMC 核对（仅新增代币）──
            cmc_verified = 0
            if enable_cmc_verify and self._cmc and tokens_saved > 0:
                if on_log:
                    on_log("=" * 40)
                    on_log("[Scanner] 阶段 ③ CMC 核对新增代币")
                cmc_verified = self._run_cmc_verify(conn, on_log)
                if on_log:
                    on_log(f"[Scanner] CMC 核对完成: {cmc_verified} 个代币")

            # ── 5. 结果 ──
            total_in_db = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
            not_listed  = conn.execute(
                "SELECT COUNT(*) FROM tokens WHERE cmc_listed=0 AND token_symbol!=''"
            ).fetchone()[0]

            db.update_scan_log(conn, scan_id,
                               status="done",
                               total_projects=total_in_db,
                               new_projects=total_saved,
                               funded_with_token=tokens_saved,
                               not_listed=not_listed,
                               cmc_verified=cmc_verified,
                               finished_at=datetime.now().isoformat())

            result = {
                "scan_id": scan_id, "status": "done",
                "total_projects": total_in_db,
                "new_projects": total_saved,
                "funded_with_token": tokens_saved,
                "not_listed": not_listed,
                "cmc_verified": cmc_verified,
            }
            if on_log:
                on_log("=" * 40)
                on_log(f"[Scanner] 增量扫描完成: {json.dumps(result, ensure_ascii=False)}")
            return result

        except Exception as e:
            logger.exception("[Scanner] 增量扫描失败")
            db.update_scan_log(conn, scan_id, status="error",
                               error_message=str(e),
                               finished_at=datetime.now().isoformat())
            if on_log:
                on_log(f"[Scanner] 扫描失败: {e}")
            return {"scan_id": scan_id, "status": "error", "error": str(e)}
        finally:
            conn.close()
