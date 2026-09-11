"""
Upbit 资方背景画像与代币拟合度打分引擎
1. 同步 Upbit 官方在线资产（公开无限制 API）
2. 构建 Upbit 资方偏好基因库 (Hashed, Dunamu, DWF, Animoca, Top VC)
3. 针对全网候选代币计算 0~100 分的 Upbit 资方拟合度 (Fit Score)
"""
import logging
import httpx
from datetime import datetime

logger = logging.getLogger("upbit-profiler")

# Upbit 核心偏好资方特征基准库 (机构名标准化映射与权重)
# Tier 1: 韩国本地与 Upbit 强关联资本 (权重 1.5)
# Tier 2: 韩国高频做市商/活跃机构 (权重 1.2)
# Tier 3: 全球顶级 Tier 1 VC (权重 1.0)
UPBIT_BASELINE_FUNDS = {
    # Tier 1: 韩系核心资本
    "hashed": {"name": "Hashed", "tier": 1, "base_weight": 0.35, "upbit_corr": 0.95},
    "dunamu & partners": {"name": "Dunamu & Partners", "tier": 1, "base_weight": 0.30, "upbit_corr": 0.98},
    "kakao ventures": {"name": "Kakao Ventures", "tier": 1, "base_weight": 0.25, "upbit_corr": 0.90},
    "kr1": {"name": "KR1", "tier": 1, "base_weight": 0.20, "upbit_corr": 0.85},
    "line ventures": {"name": "LINE Ventures", "tier": 1, "base_weight": 0.20, "upbit_corr": 0.85},

    # Tier 2: 韩国做市商与高频参投方
    "dwf labs": {"name": "DWF Labs", "tier": 2, "base_weight": 0.25, "upbit_corr": 0.88},
    "animoca brands": {"name": "Animoca Brands", "tier": 2, "base_weight": 0.25, "upbit_corr": 0.85},
    "wintermute": {"name": "Wintermute", "tier": 2, "base_weight": 0.20, "upbit_corr": 0.82},
    "jump crypto": {"name": "Jump Crypto", "tier": 2, "base_weight": 0.20, "upbit_corr": 0.80},
    "spartan group": {"name": "Spartan Group", "tier": 2, "base_weight": 0.18, "upbit_corr": 0.78},
    "hashkey capital": {"name": "HashKey Capital", "tier": 2, "base_weight": 0.18, "upbit_corr": 0.78},

    # Tier 3: 全球一线顶级 VC
    "a16z crypto": {"name": "a16z crypto", "tier": 3, "base_weight": 0.22, "upbit_corr": 0.85},
    "andreessen horowitz": {"name": "a16z crypto", "tier": 3, "base_weight": 0.22, "upbit_corr": 0.85},
    "polychain capital": {"name": "Polychain Capital", "tier": 3, "base_weight": 0.20, "upbit_corr": 0.82},
    "coinbase ventures": {"name": "Coinbase Ventures", "tier": 3, "base_weight": 0.20, "upbit_corr": 0.80},
    "pantera capital": {"name": "Pantera Capital", "tier": 3, "base_weight": 0.18, "upbit_corr": 0.78},
    "multicoin capital": {"name": "Multicoin Capital", "tier": 3, "base_weight": 0.18, "upbit_corr": 0.78},
    "dragonfly": {"name": "Dragonfly", "tier": 3, "base_weight": 0.18, "upbit_corr": 0.78},
    "dragonfly capital": {"name": "Dragonfly", "tier": 3, "base_weight": 0.18, "upbit_corr": 0.78},
    "paradigm": {"name": "Paradigm", "tier": 3, "base_weight": 0.20, "upbit_corr": 0.82},
    "binance labs": {"name": "Binance Labs", "tier": 3, "base_weight": 0.18, "upbit_corr": 0.75},
    "delphi digital": {"name": "Delphi Digital", "tier": 3, "base_weight": 0.16, "upbit_corr": 0.75},
    "framework ventures": {"name": "Framework Ventures", "tier": 3, "base_weight": 0.15, "upbit_corr": 0.72},
    "galaxy digital": {"name": "Galaxy Digital", "tier": 3, "base_weight": 0.15, "upbit_corr": 0.72},
    "sequoia capital": {"name": "Sequoia Capital", "tier": 3, "base_weight": 0.18, "upbit_corr": 0.75},
}


class UpbitProfiler:
    """Upbit 市场数据与拟合度计算引擎。"""

    def __init__(self, api_base: str = "https://api.upbit.com/v1", proxy: str | None = None):
        self.api_base = api_base.rstrip("/")
        self.proxy = proxy
        self._cached_symbols: set[str] = set()
        self._cached_markets: list[dict] = []
        self._last_sync_time: float = 0.0

    def fetch_upbit_markets(self, force_refresh: bool = False) -> tuple[list[dict], set[str]]:
        """获取 Upbit 全量在线交易对与代币 Symbol。"""
        import time
        now = time.time()
        if not force_refresh and self._cached_symbols and (now - self._last_sync_time < 3600):
            return self._cached_markets, self._cached_symbols

        url = f"{self.api_base}/market/all?isDetails=true"
        try:
            with httpx.Client(proxy=self.proxy, timeout=15.0, verify=False) as client:
                resp = client.get(url)
            if resp.status_code == 200:
                markets = resp.json()
                symbols = set()
                parsed = []
                for m in markets:
                    market_code = m.get("market", "")
                    if "-" in market_code:
                        sym = market_code.split("-")[1].upper()
                        symbols.add(sym)
                        parsed.append({
                            "market": market_code,
                            "symbol": sym,
                            "korean_name": m.get("korean_name", ""),
                            "english_name": m.get("english_name", ""),
                        })
                self._cached_markets = parsed
                self._cached_symbols = symbols
                self._last_sync_time = now
                logger.info("[Upbit Profiler] 成功拉取 Upbit 市场: %d 个交易对, %d 个代币 Symbol",
                            len(parsed), len(symbols))
                return parsed, symbols
            else:
                logger.error("[Upbit Profiler] 拉取失败: HTTP %d", resp.status_code)
        except Exception as e:
            logger.error("[Upbit Profiler] 网络异常: %s", e)

        return self._cached_markets, self._cached_symbols

    def calculate_fit_score(self, token_symbol: str, investors_raw: list | str | None) -> dict:
        """
        计算代币与 Upbit 资方偏好的拟合度得分 (0~100)。
        :param token_symbol: 代币符号 (例如 'APT', 'SUI', 'BERA')
        :param investors_raw: 投资机构列表 (list of names/slugs 或逗号分隔字符串)
        :return: 包含 score, level, matched_backers, is_candidate, is_upbit_listed
        """
        sym_clean = (token_symbol or "").strip().upper()
        _, upbit_symbols = self.fetch_upbit_markets()
        is_upbit_listed = sym_clean in upbit_symbols if sym_clean else False

        # 解析标准化资方列表
        investor_list = []
        if isinstance(investors_raw, list):
            for inv in investors_raw:
                if isinstance(inv, dict):
                    name = inv.get("name") or inv.get("slug") or ""
                else:
                    name = str(inv)
                if name:
                    investor_list.append(name.strip())
        elif isinstance(investors_raw, str) and investors_raw.strip():
            import re
            parts = re.split(r"[,;|/]", investors_raw)
            investor_list = [p.strip() for p in parts if p.strip()]

        matched = []
        raw_score = 0.0

        for inv in investor_list:
            inv_lower = inv.lower()
            # 模糊/精确匹配已知资方库
            matched_key = None
            for base_key in UPBIT_BASELINE_FUNDS:
                if base_key in inv_lower or inv_lower in base_key:
                    matched_key = base_key
                    break

            if matched_key:
                info = UPBIT_BASELINE_FUNDS[matched_key]
                tier_mult = 1.6 if info["tier"] == 1 else (1.3 if info["tier"] == 2 else 1.0)
                score_contrib = info["base_weight"] * info["upbit_corr"] * tier_mult * 100.0
                raw_score += score_contrib
                matched.append({
                    "name": info["name"],
                    "tier": info["tier"],
                    "weight": round(info["base_weight"], 2),
                    "contribution": round(score_contrib, 1)
                })

        # 联合参投加成 (多顶级机构协同效应)
        if len(matched) >= 3:
            raw_score *= 1.25
        elif len(matched) == 2:
            raw_score *= 1.15

        final_score = int(min(100.0, max(0.0, round(raw_score))))

        # 评级等级
        if final_score >= 85:
            level = "S"
        elif final_score >= 70:
            level = "A"
        elif final_score >= 50:
            level = "B"
        else:
            level = "C"

        is_candidate = (final_score >= 70) and (not is_upbit_listed)

        return {
            "token_symbol": sym_clean,
            "fit_score": final_score,
            "fit_level": level,
            "is_upbit_listed": is_upbit_listed,
            "is_candidate": is_candidate,
            "matched_backers": matched,
            "total_investors_count": len(investor_list)
        }
