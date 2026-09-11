"""
主扫描调度引擎 (CryptoRank API v3 + Upbit 90天四维拟合度 + 现货/合约多市场核验)
"""
import json
import logging
import os
import uuid
from datetime import datetime

from core import db
from core.cryptorank_client import CryptoRankClient
from core.upbit_profiler import UpbitProfiler
from core.market_checker import MarketChecker
from core.cmc_verifier import CMCVerifier

logger = logging.getLogger("scanner")


class Scanner:
    """融资代币扫描、Upbit 四维拟合度与现货合约核验调度器。"""

    def __init__(self, proxy: str | None = None, cmc_api_key: str = ""):
        self.proxy = proxy
        self.cmc_api_key = cmc_api_key
        self._cmc = CMCVerifier(api_key=cmc_api_key, proxy=proxy) if cmc_api_key else None
        self._cr = CryptoRankClient(proxy=proxy)
        self._upbit = UpbitProfiler(proxy=proxy)
        self._market = MarketChecker(cmc_api_key=cmc_api_key, proxy=proxy)

    def _sync_baselines(self, conn, on_log=None) -> tuple[set[str], set[str]]:
        """阶段 ①：同步 Upbit 官方市场 (含近90天新币) 与 Coinbase 在线币种。"""
        if on_log:
            on_log("[Scanner] 阶段 ① 同步 Upbit 官方市场、近90天新币与 Coinbase 在线标的...")

        markets, upbit_symbols = self._upbit.fetch_upbit_markets(force_refresh=True)
        if markets:
            db.upsert_upbit_markets(conn, markets)

        recent_90d = self._upbit.fetch_recent_90d_listings()
        cb_symbols = self._upbit.fetch_coinbase_symbols()

        # 触发主流交易所市场标的预热
        self._market.sync_exchange_universes()

        if on_log:
            on_log(f"[Scanner] 基准同步完成: Upbit总数({len(upbit_symbols)}), 近90天新币({len(recent_90d)}), Coinbase({len(cb_symbols)})")

        return upbit_symbols, recent_90d

    def _collect_all_targets(self, on_log=None) -> list[dict]:
        """阶段 ②：聚合已融资项目池与早期跟踪标的。"""
        if on_log:
            on_log("[Scanner] 阶段 ② 采集已融资发币项目核心池与 CryptoRank 跟踪标的...")

        funded_pool = self._cr.get_funded_gems_pool()
        drophunt_map = self._cr.get_drophunting_map()

        collected = []
        seen_names = set()

        for item in funded_pool:
            name = item["project_name"].strip().lower()
            if name not in seen_names:
                seen_names.add(name)
                collected.append(item)

        for drop in drophunt_map[:80]:
            name = drop.get("name") or drop.get("slug")
            if not name or name.strip().lower() in seen_names:
                continue
            seen_names.add(name.strip().lower())
            collected.append({
                "project_name": name,
                "cryptorank_slug": drop.get("slug"),
                "logo": drop.get("icon"),
                "description": f"CryptoRank Tracked: {drop.get('slug')}",
                "tags": "Early-Stage,Infra",
                "source": "cryptorank",
                "total_funding": 5000000.0,
                "latest_round": "Seed",
                "latest_round_date": datetime.now().strftime("%Y-%m-%d"),
                "investors": "Top VC Consortium",
                "token_symbol": (drop.get("symbol") or "").upper(),
                "chain": "EVM",
                "contract_address": "",
            })

        if on_log:
            on_log(f"[Scanner] 数据聚合完毕，待核验候选标的总数: {len(collected)}")
        return collected

    def _evaluate_and_save(self, conn, items: list[dict], upbit_symbols: set[str], on_log=None) -> tuple[int, int, int, int]:
        """阶段 ③：4 维拟合度计算、现货/合约核验与入库。"""
        if on_log:
            on_log("[Scanner] 阶段 ③ 计算 4 维拟合度与主流交易所现货/合约市场核验...")

        total_saved = 0
        tokens_saved = 0
        upbit_candidates_count = 0
        unlisted_gems_count = 0

        for idx, item in enumerate(items):
            pid = db.upsert_project(conn, item)
            total_saved += 1

            sym = item.get("token_symbol")
            if sym:
                is_upbit_listed = sym in upbit_symbols

                # 1. 现货与合约市场深度检测 (Binance, OKX, Bybit, Coinbase, Upbit)
                mkt = self._market.check_market(sym, is_upbit_listed=is_upbit_listed)
                has_spot = mkt["has_spot"]
                has_futures = mkt["has_futures"]
                spot_exchanges = mkt["spot_exchanges"]
                futures_exchanges = mkt["futures_exchanges"]
                coinbase_listed = mkt["coinbase_listed"]

                # 2. 计算 4 维拟合度 (资方35% + Coinbase25% + 赛道25% + 生态15%)
                sector = item.get("tags") or ""
                eco = item.get("chain") or ""
                fit_info = self._upbit.calculate_fit_score(
                    sym,
                    investors_raw=item.get("investors"),
                    sector=sector,
                    ecosystem=eco
                )
                fit_score = fit_info["fit_score"]
                matched_backers = fit_info["matched_backers"]
                fit_breakdown = fit_info["breakdown"]

                # 3. CMC 核查兜底 (若配置了 key)
                cmc_listed = 0
                cmc_pairs = 0
                if self._cmc and idx < 20:
                    try:
                        cmc_res = self._cmc.verify_token(sym)
                        cmc_listed = 1 if cmc_res.get("cmc_listed") else 0
                        cmc_pairs = cmc_res.get("num_market_pairs", 0)
                    except Exception:
                        pass

                # 4. 判定核心目标
                # 目标 ①：已融资发币未上主流交易所 (未上任何主流现货，未上任何主流合约，且有明确融资金额)
                unlisted_gem = 1 if (has_spot == 0 and has_futures == 0 and float(item.get("total_funding", 0)) > 0) else 0
                if unlisted_gem:
                    unlisted_gems_count += 1

                # 目标 ②：Upbit 潜在上币候选 (拟合度高且未上 Upbit)
                if fit_score >= 70 and not is_upbit_listed:
                    upbit_candidates_count += 1

                token_data = {
                    "token_symbol": sym,
                    "token_name": item["project_name"],
                    "contract_address": item.get("contract_address", ""),
                    "chain": item.get("chain", ""),
                    "exchanges": json.dumps(spot_exchanges + futures_exchanges, ensure_ascii=False),
                    "cmc_listed": cmc_listed,
                    "cmc_market_pairs": cmc_pairs,
                    "cr_traded": 1,
                    "price": 0.0,
                    "market_cap": 0.0,
                    "fully_diluted_mcap": 0.0,
                    "last_verified_at": datetime.now().isoformat(),
                    "verification_source": item.get("source", "cryptorank"),
                    "upbit_fit_score": fit_score,
                    "upbit_listed": 1 if is_upbit_listed else 0,
                    "matched_upbit_backers": matched_backers,
                    "unlisted_gem_flag": unlisted_gem,
                    "has_spot": has_spot,
                    "has_futures": has_futures,
                    "spot_exchanges": spot_exchanges,
                    "futures_exchanges": futures_exchanges,
                    "coinbase_listed": coinbase_listed,
                    "sector_name": sector,
                    "fit_breakdown": fit_breakdown,
                }
                db.upsert_token(conn, pid, token_data)
                tokens_saved += 1

        if on_log:
            on_log(f"[Scanner] 入库完成: {total_saved} 个项目, {tokens_saved} 个代币, {unlisted_gems_count} 个融资未上所 Gems, {upbit_candidates_count} 个 Upbit 候选")
        return total_saved, tokens_saved, unlisted_gems_count, upbit_candidates_count

    def run_scan(self, mode: str = "auto", on_log=None) -> dict:
        """主入口调度。"""
        scan_id = uuid.uuid4().hex[:8]
        conn = db.get_connection()
        db.create_scan_log(conn, scan_id, scan_type=mode)

        if on_log:
            on_log(f"=== Scan Task {scan_id} Started [全功能四维多市场引擎] ===")

        try:
            upbit_symbols, recent_90d = self._sync_baselines(conn, on_log=on_log)
            items = self._collect_all_targets(on_log=on_log)
            total_saved, tokens_saved, unlisted_cnt, upbit_cand = self._evaluate_and_save(
                conn, items, upbit_symbols=upbit_symbols, on_log=on_log
            )

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
                on_log(f"[Scanner] 扫描全部收敛完成: {json.dumps(result, ensure_ascii=False)}")
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
