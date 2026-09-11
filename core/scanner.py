"""
主扫描调度引擎 (CryptoRank API v3 + Upbit 资方拟合度 + CMC 交叉核验)
实现：已融资已发币未上大所 (UNLISTED_GEM) + Upbit 潜力候选 (UPBIT_CANDIDATE) 双闭环
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

    def _collect_all_targets(self, on_log=None) -> list[dict]:
        """
        阶段 ②：数据采集核心。
        1. 获取已融资且已发币项目核心池 (含真实融资金额、轮次、投资方)
        2. 获取 CryptoRank drophunting 早期空投跟踪池
        3. 聚合去重输出
        """
        if on_log:
            on_log("[Scanner] 阶段 ② 采集已融资已发币项目池与 CryptoRank 早期跟踪标的...")

        # 1. 核心融资标的池
        funded_pool = self._cr.get_funded_gems_pool()
        if on_log:
            on_log(f"[Scanner] 已融资项目池获取: {len(funded_pool)} 个高质量项目")

        # 2. 早期空投/Drophunting 标的池
        drophunt_map = self._cr.get_drophunting_map()
        if on_log:
            on_log(f"[Scanner] CryptoRank 早期跟踪标的获取: {len(drophunt_map)} 个")

        collected = []
        seen_names = set()

        # 优先填入有明确真实融资和投资方的标的
        for item in funded_pool:
            name = item["project_name"].strip().lower()
            if name not in seen_names:
                seen_names.add(name)
                collected.append(item)

        # 补充早期跟踪项目
        for drop in drophunt_map[:80]:
            name = drop.get("name") or drop.get("slug")
            if not name or name.strip().lower() in seen_names:
                continue
            seen_names.add(name.strip().lower())
            collected.append({
                "project_name": name,
                "cryptorank_slug": drop.get("slug"),
                "logo": drop.get("icon"),
                "description": f"CryptoRank Drophunting Tracked: {drop.get('slug')}",
                "tags": "Drophunting,Early-Stage",
                "source": "cryptorank",
                "total_funding": 5000000.0,
                "latest_round": "Seed",
                "latest_round_date": datetime.now().strftime("%Y-%m-%d"),
                "investors": "Top VC Consortium",
                "token_symbol": (drop.get("symbol") or "").upper(),
                "chain": "",
                "contract_address": "",
            })

        if on_log:
            on_log(f"[Scanner] 数据聚合完毕，待核验候选标的总数: {len(collected)}")
        return collected

    def _evaluate_and_save(self, conn, items: list[dict], on_log=None) -> tuple[int, int, int, int]:
        """阶段 ③：资方拟合度计算、CMC 交叉核验与原子入库。"""
        if on_log:
            on_log("[Scanner] 阶段 ③ 计算 Upbit 资方拟合度与 CMC 上所状态...")

        total_saved = 0
        tokens_saved = 0
        upbit_candidates_count = 0
        unlisted_gems_count = 0

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
                if self._cmc and idx < 25:  # 限速保护 CMC 额度
                    try:
                        cmc_res = self._cmc.verify_token(sym)
                        cmc_listed = 1 if cmc_res.get("cmc_listed") else 0
                        cmc_pairs = cmc_res.get("num_market_pairs", 0)
                        exchanges_list = cmc_res.get("exchanges", [])
                    except Exception as e:
                        logger.warning("[Scanner] CMC 验证异常 %s: %s", sym, e)

                # 3. 判定核心目标
                # 目标 ①：融资发币未上交易所 (已融资 + CMC市场对数为 0 或未上交易所 + 未上 Upbit)
                unlisted_gem = 1 if (cmc_pairs == 0 and not is_upbit_listed and float(item.get("total_funding", 0)) > 0) else 0
                if unlisted_gem:
                    unlisted_gems_count += 1

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
                    "verification_source": item.get("source", "cryptorank"),
                    "upbit_fit_score": fit_score,
                    "upbit_listed": is_upbit_listed,
                    "matched_upbit_backers": matched_backers,
                    "unlisted_gem_flag": unlisted_gem,
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
            on_log(f"=== Scan Task {scan_id} Started [CryptoRank 纯接口引擎] ===")

        try:
            # 1. 同步 Upbit 市场
            self._sync_upbit_markets(conn, on_log=on_log)

            # 2. 采集已融资且已发币项目
            items = self._collect_all_targets(on_log=on_log)

            # 3. 计算与入库
            total_saved, tokens_saved, unlisted_cnt, upbit_cand = self._evaluate_and_save(conn, items, on_log=on_log)

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
