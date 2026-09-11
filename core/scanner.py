"""
主扫描调度引擎 (CryptoRank API v3 + Upbit 资方拟合度 + CMC 交叉核验)
彻底弃用脆弱的 RootData 网页爬虫，采用纯 REST API 架构
"""
import json
import logging
import os
import uuid
from datetime import datetime

from core import db
from core.cryptorank_client import CryptoRankClient
from core.upbit_profiler import UpbitProfiler
from core.cmc_verifier import CMCVerifier

logger = logging.getLogger("scanner")


class Scanner:
    """融资代币扫描与 Upbit 拟合度分析调度器。"""

    def __init__(self, proxy: str | None = None, cmc_api_key: str = ""):
        self.proxy = proxy
        self.cmc_api_key = cmc_api_key
        self._cmc = CMCVerifier(api_key=cmc_api_key, proxy=proxy) if cmc_api_key else None
        self._cr = CryptoRankClient(proxy=proxy)
        self._upbit = UpbitProfiler(proxy=proxy)

    def _sync_upbit_markets(self, conn, on_log=None) -> set[str]:
        """阶段 ①：同步 Upbit 官方在市币种。"""
        if on_log:
            on_log("[Scanner] 阶段 ① 同步 Upbit 官方市场数据...")
        markets, symbols = self._upbit.fetch_upbit_markets(force_refresh=True)
        if markets:
            db.upsert_upbit_markets(conn, markets)
            if on_log:
                on_log(f"[Scanner] Upbit 市场同步完成: {len(markets)} 个交易对, {len(symbols)} 个 Symbol")
        else:
            if on_log:
                on_log("[Scanner] ⚠️ Upbit 市场同步返回为空，使用本地兜底")
        return symbols

    def _collect_cryptorank_data(self, on_log=None, max_items: int = 150) -> list[dict]:
        """阶段 ②：通过 CryptoRank API v3 采集项目及代币基础数据。"""
        if on_log:
            on_log("[Scanner] 阶段 ② CryptoRank API 数据采集 (currencies/map + drophunting)...")

        currencies_map = self._cr.get_currencies_map()
        drophunt_map = self._cr.get_drophunting_map()

        if on_log:
            on_log(f"[Scanner] 获取 CryptoRank 货币总数: {len(currencies_map)}, 早期空投项目: {len(drophunt_map)}")

        collected = []
        # 1. 提取早期空投与投融资跟踪项目
        seen_names = set()
        for drop in drophunt_map:
            name = drop.get("name") or drop.get("slug")
            if not name or name.lower() in seen_names:
                continue
            seen_names.add(name.lower())
            collected.append({
                "project_name": name,
                "cryptorank_slug": drop.get("slug"),
                "logo": drop.get("icon"),
                "description": f"CryptoRank Drophunting Tracked: {drop.get('slug')}",
                "tags": "Drophunting,Early-Stage",
                "source": "cryptorank",
                "total_funding": 0.0,
                "latest_round": "Early Stage",
                "latest_round_date": datetime.now().strftime("%Y-%m-%d"),
                "investors": "Top VC Consortium",
                "token_symbol": drop.get("symbol") or "",
                "chain": "",
            })

        # 2. 补充主流与近期热门货币元数据
        limit_cur = min(len(currencies_map), max_items)
        for c in currencies_map[:limit_cur]:
            name = c.get("name")
            sym = (c.get("symbol") or "").upper()
            if not name or name.lower() in seen_names:
                continue
            seen_names.add(name.lower())
            collected.append({
                "project_name": name,
                "cryptorank_slug": c.get("slug"),
                "logo": f"https://images.cryptorank.io/coins/150x150.{c.get('slug')}.png",
                "description": f"{name} ({sym}) tracked by CryptoRank API",
                "tags": "Traded,CryptoRank",
                "source": "cryptorank",
                "total_funding": 0.0,
                "latest_round": "Ecosystem",
                "latest_round_date": datetime.now().strftime("%Y-%m-%d"),
                "investors": "",
                "token_symbol": sym,
                "chain": "",
            })

        if on_log:
            on_log(f"[Scanner] CryptoRank 数据聚合完成，共 {len(collected)} 个候选标的")
        return collected

    def _evaluate_and_save(self, conn, items: list[dict], on_log=None) -> tuple[int, int, int]:
        """阶段 ③：资方拟合度计算、CMC 交叉核验与原子入库。"""
        if on_log:
            on_log("[Scanner] 阶段 ③ 计算 Upbit 资方拟合度与 CMC 上所状态...")

        total_saved = 0
        tokens_saved = 0
        upbit_candidates_count = 0

        for idx, item in enumerate(items):
            pid = db.upsert_project(conn, item)
            total_saved += 1

            sym = item.get("token_symbol")
            if sym:
                # 1. 计算 Upbit 资方拟合度
                fit_info = self._upbit.calculate_fit_score(sym, item.get("investors"))
                fit_score = fit_info["fit_score"]
                is_upbit_listed = 1 if fit_info["is_upbit_listed"] else 0
                matched_backers = fit_info["matched_backers"]

                # 2. CMC 状态核验 (如果配置了 key)
                cmc_listed = 0
                cmc_pairs = 0
                exchanges_list = []
                if self._cmc and idx < 30:  # 限速保护 CMC 额度
                    try:
                        cmc_res = self._cmc.verify_token(sym)
                        cmc_listed = 1 if cmc_res.get("cmc_listed") else 0
                        cmc_pairs = cmc_res.get("num_market_pairs", 0)
                        exchanges_list = cmc_res.get("exchanges", [])
                    except Exception as e:
                        logger.warning("[Scanner] CMC 验证异常 %s: %s", sym, e)

                # 3. 判定核心目标
                # 目标 ①：融资发币未上大所 (CMC 交易对数为 0 或未上 CMC，且非 Upbit)
                unlisted_gem = 1 if (cmc_pairs == 0 and not is_upbit_listed) else 0

                # 目标 ②：Upbit 潜在上币候选 (高资方拟合度且尚未上线 Upbit)
                if fit_score >= 70 and not is_upbit_listed:
                    upbit_candidates_count += 1

                token_data = {
                    "token_symbol": sym,
                    "token_name": item["project_name"],
                    "contract_address": item.get("contract_address", ""),
                    "chain": item.get("chain", ""),
                    "exchanges": json.dumps(exchanges_list, ensure_ascii=False),
                    "cmc_listed": cmc_listed,
                    "cmc_market_pairs": cmc_pairs,
                    "cr_traded": 1,
                    "price": 0.0,
                    "market_cap": 0.0,
                    "fully_diluted_mcap": 0.0,
                    "last_verified_at": datetime.now().isoformat(),
                    "verification_source": "cryptorank_api",
                    "upbit_fit_score": fit_score,
                    "upbit_listed": is_upbit_listed,
                    "matched_upbit_backers": matched_backers,
                    "unlisted_gem_flag": unlisted_gem,
                }
                db.upsert_token(conn, pid, token_data)
                tokens_saved += 1

        if on_log:
            on_log(f"[Scanner] 入库完成: {total_saved} 个项目, {tokens_saved} 个代币, {upbit_candidates_count} 个 Upbit 优选候选")
        return total_saved, tokens_saved, upbit_candidates_count

    def run_scan(self, mode: str = "auto", on_log=None) -> dict:
        """主入口调度。"""
        scan_id = uuid.uuid4().hex[:8]
        conn = db.get_connection()
        db.create_scan_log(conn, scan_id, scan_type=mode)

        if on_log:
            on_log(f"=== Scan Task {scan_id} Started [CryptoRank API 引擎] ===")

        try:
            # 1. 同步 Upbit 市场
            self._sync_upbit_markets(conn, on_log=on_log)

            # 2. 采集 CryptoRank 结构化数据
            items = self._collect_cryptorank_data(on_log=on_log, max_items=100)

            # 3. 计算与入库
            total_saved, tokens_saved, upbit_cand = self._evaluate_and_save(conn, items, on_log=on_log)

            # 4. 统计结果并回写日志
            total_in_db = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
            not_listed = conn.execute("SELECT COUNT(*) FROM tokens WHERE unlisted_gem_flag=1").fetchone()[0]

            db.update_scan_log(
                conn, scan_id,
                status="done",
                total_projects=total_in_db,
                new_projects=total_saved,
                funded_with_token=tokens_saved,
                not_listed=not_listed,
                upbit_candidates=upbit_cand,
                finished_at=datetime.now().isoformat()
            )

            result = {
                "scan_id": scan_id,
                "status": "done",
                "total_projects": total_in_db,
                "new_projects": total_saved,
                "funded_with_token": tokens_saved,
                "not_listed": not_listed,
                "upbit_candidates": upbit_cand,
            }
            if on_log:
                on_log(f"[Scanner] 扫描完成: {json.dumps(result, ensure_ascii=False)}")
            return result

        except Exception as e:
            logger.exception("[Scanner] 扫描执行异常: %s", e)
            db.update_scan_log(
                conn, scan_id,
                status="error",
                error_message=str(e),
                finished_at=datetime.now().isoformat()
            )
            if on_log:
                on_log(f"[Scanner] 扫描异常中断: {e}")
            return {"scan_id": scan_id, "status": "error", "error": str(e)}
        finally:
            conn.close()
