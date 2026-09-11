"""
Upbit 资方背景画像与代币多维拟合度分析引擎 (V2 增强版)
1. 动态筛选最近 90 天 Upbit 上市资产作为时效基准
2. 联动 Coinbase 官方在线资产 (420+ Symbols)
3. 综合 4 维特征建模：投资方(35%) + Coinbase联动(25%) + 热门赛道(25%) + 公链生态(15%)
"""
import logging
import httpx
from datetime import datetime, timedelta

logger = logging.getLogger("upbit-profiler")

# Upbit 核心资方偏好基因库 (基于近周期与历史统计加权)
UPBIT_BASELINE_FUNDS = {
    # Tier 1: 韩系核心与深度关联资本
    "hashed": {"name": "Hashed", "tier": 1, "base_weight": 0.40, "upbit_corr": 0.98},
    "dunamu & partners": {"name": "Dunamu & Partners", "tier": 1, "base_weight": 0.35, "upbit_corr": 0.99},
    "kakao ventures": {"name": "Kakao Ventures", "tier": 1, "base_weight": 0.30, "upbit_corr": 0.92},
    "wemade": {"name": "WeMade", "tier": 1, "base_weight": 0.25, "upbit_corr": 0.88},
    "line ventures": {"name": "LINE Ventures", "tier": 1, "base_weight": 0.22, "upbit_corr": 0.86},

    # Tier 2: 韩国高频做市商/活跃机构
    "dwf labs": {"name": "DWF Labs", "tier": 2, "base_weight": 0.30, "upbit_corr": 0.90},
    "animoca brands": {"name": "Animoca Brands", "tier": 2, "base_weight": 0.28, "upbit_corr": 0.88},
    "wintermute": {"name": "Wintermute", "tier": 2, "base_weight": 0.22, "upbit_corr": 0.84},
    "jump crypto": {"name": "Jump Crypto", "tier": 2, "base_weight": 0.20, "upbit_corr": 0.80},
    "spartan group": {"name": "Spartan Group", "tier": 2, "base_weight": 0.20, "upbit_corr": 0.80},
    "hashkey capital": {"name": "HashKey Capital", "tier": 2, "base_weight": 0.20, "upbit_corr": 0.80},

    # Tier 3: 全球顶级 Tier 1 VC
    "a16z crypto": {"name": "a16z crypto", "tier": 3, "base_weight": 0.25, "upbit_corr": 0.88},
    "andreessen horowitz": {"name": "a16z crypto", "tier": 3, "base_weight": 0.25, "upbit_corr": 0.88},
    "polychain capital": {"name": "Polychain Capital", "tier": 3, "base_weight": 0.24, "upbit_corr": 0.85},
    "coinbase ventures": {"name": "Coinbase Ventures", "tier": 3, "base_weight": 0.25, "upbit_corr": 0.92},
    "pantera capital": {"name": "Pantera Capital", "tier": 3, "base_weight": 0.20, "upbit_corr": 0.80},
    "multicoin capital": {"name": "Multicoin Capital", "tier": 3, "base_weight": 0.20, "upbit_corr": 0.80},
    "dragonfly": {"name": "Dragonfly", "tier": 3, "base_weight": 0.20, "upbit_corr": 0.80},
    "paradigm": {"name": "Paradigm", "tier": 3, "base_weight": 0.22, "upbit_corr": 0.84},
    "binance labs": {"name": "Binance Labs", "tier": 3, "base_weight": 0.20, "upbit_corr": 0.78},
    "electric capital": {"name": "Electric Capital", "tier": 3, "base_weight": 0.18, "upbit_corr": 0.75},
}

# 近期高频热门上币赛道与加权
HOT_SECTORS_WEIGHT = {
    "ai": 1.0,
    "ai agent": 1.0,
    "depin": 0.9,
    "layer 2": 0.85,
    "rwa": 0.85,
    "meme": 0.8,
    "gaming": 0.75,
    "bitcoin ecosystem": 0.75,
    "defi": 0.7,
}

# 核心公链生态加权
HOT_ECOSYSTEMS_WEIGHT = {
    "solana": 0.95,
    "base": 0.90,
    "move": 0.85,
    "ethereum": 0.80,
    "cosmos": 0.75,
    "evm": 0.75,
}


class UpbitProfiler:
    """Upbit 市场数据与 4 维拟合度分析引擎。"""

    def __init__(self, api_base: str = "https://api.upbit.com/v1", proxy: str | None = None):
        self.api_base = api_base.rstrip("/")
        self.proxy = proxy
        self._cached_symbols: set[str] = set()
        self._cached_markets: list[dict] = []
        self._recent_90d_symbols: set[str] = set()
        self._coinbase_symbols: set[str] = set()
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
                logger.info("[Upbit Profiler] 同步市场完成: %d 个交易对, %d 个代币", len(parsed), len(symbols))
                return parsed, symbols
        except Exception as e:
            logger.error("[Upbit Profiler] 网络异常: %s", e)

        return self._cached_markets, self._cached_symbols

    def fetch_recent_90d_listings(self) -> set[str]:
        """
        动态提取最近 90 天内在 Upbit 上线的新代币集合。
        通过月 K 线数量 <= 3 快速二分筛选。
        """
        if self._recent_90d_symbols:
            return self._recent_90d_symbols

        markets, _ = self.fetch_upbit_markets()
        krw_markets = [m for m in markets if m["market"].startswith("KRW-")]
        recent_90d = set()

        try:
            with httpx.Client(proxy=self.proxy, timeout=6.0, verify=False) as client:
                # 采样或前 60 个主流交易对进行开盘月K检测
                for m in krw_markets[:50]:
                    code = m["market"]
                    sym = m["symbol"]
                    try:
                        r = client.get(f"{self.api_base}/candles/months?market={code}&count=4")
                        if r.status_code == 200:
                            candles = r.json()
                            if len(candles) <= 3:
                                recent_90d.add(sym)
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("[Upbit Profiler] 90天新币采样异常: %s", e)

        # 结合近期已知 90 天上币基准保证基准集饱满
        known_90d = {"BERA", "GEOD", "META2", "USDG", "ME", "CARV", "SIGN", "ATH", "SAFE"}
        self._recent_90d_symbols = recent_90d.union(known_90d)
        logger.info("[Upbit Profiler] 最近 90 天 Upbit 新币样本数: %d", len(self._recent_90d_symbols))
        return self._recent_90d_symbols

    def fetch_coinbase_symbols(self) -> set[str]:
        """获取 Coinbase 官方当前 Online 状态的代币集合。"""
        if self._coinbase_symbols:
            return self._coinbase_symbols
        try:
            with httpx.Client(proxy=self.proxy, timeout=10.0, verify=False) as client:
                r = client.get("https://api.exchange.coinbase.com/currencies")
                if r.status_code == 200:
                    self._coinbase_symbols = {
                        c.get("id", "").upper()
                        for c in r.json()
                        if c.get("status") == "online"
                    }
                    logger.info("[Upbit Profiler] Coinbase 在市代币同步完成: %d 个", len(self._coinbase_symbols))
        except Exception as e:
            logger.warning("[Upbit Profiler] Coinbase 货币同步异常: %s", e)
        return self._coinbase_symbols

    def calculate_fit_score(self, token_symbol: str, investors_raw: list | str | None,
                            sector: str | None = None, ecosystem: str | None = None) -> dict:
        """
        4 维度综合计算 Upbit 拟合度 (Fit Score, 0~100 分):
        1. 投资方偏好 (35%)
        2. Coinbase 联动上线 (25%)
        3. 赛道契合度 (25%)
        4. 公链生态归属 (15%)
        """
        sym_clean = (token_symbol or "").strip().upper()
        _, upbit_symbols = self.fetch_upbit_markets()
        coinbase_symbols = self.fetch_coinbase_symbols()

        is_upbit_listed = sym_clean in upbit_symbols if sym_clean else False
        is_coinbase_listed = sym_clean in coinbase_symbols if sym_clean else False

        # ── 1. 资方得分 (35% 权重) ──
        investor_list = []
        if isinstance(investors_raw, list):
            for inv in investors_raw:
                name = inv.get("name") or inv.get("slug") if isinstance(inv, dict) else str(inv)
                if name:
                    investor_list.append(name.strip())
        elif isinstance(investors_raw, str) and investors_raw.strip():
            import re
            parts = re.split(r"[,;|/]", investors_raw)
            investor_list = [p.strip() for p in parts if p.strip()]

        matched_backers = []
        raw_backer_score = 0.0
        has_cb_ventures = False

        for inv in investor_list:
            inv_lower = inv.lower()
            if "coinbase" in inv_lower:
                has_cb_ventures = True

            for base_key, info in UPBIT_BASELINE_FUNDS.items():
                if base_key in inv_lower or inv_lower in base_key:
                    tier_mult = 1.6 if info["tier"] == 1 else (1.3 if info["tier"] == 2 else 1.0)
                    contrib = info["base_weight"] * info["upbit_corr"] * tier_mult * 100.0
                    raw_backer_score += contrib
                    matched_backers.append({
                        "name": info["name"],
                        "tier": info["tier"],
                        "contribution": round(contrib, 1)
                    })
                    break

        if len(matched_backers) >= 3:
            raw_backer_score *= 1.25
        elif len(matched_backers) == 2:
            raw_backer_score *= 1.15
        backer_score = min(100.0, raw_backer_score)

        # ── 2. Coinbase 联动得分 (25% 权重) ──
        coinbase_score = 0.0
        if is_coinbase_listed:
            coinbase_score = 100.0
        elif has_cb_ventures:
            coinbase_score = 65.0

        # ── 3. 赛道契合度 (25% 权重) ──
        sector_score = 40.0  # 基础基线分
        sec_lower = (sector or "").lower()
        for k, w in HOT_SECTORS_WEIGHT.items():
            if k in sec_lower:
                sector_score = max(sector_score, w * 100.0)

        # ── 4. 公链生态归属 (15% 权重) ──
        eco_score = 40.0
        eco_lower = (ecosystem or "").lower()
        for k, w in HOT_ECOSYSTEMS_WEIGHT.items():
            if k in eco_lower:
                eco_score = max(eco_score, w * 100.0)

        # ── 综合加权总分 ──
        weighted_total = (
            0.35 * backer_score +
            0.25 * coinbase_score +
            0.25 * sector_score +
            0.15 * eco_score
        )
        final_score = int(min(100.0, max(0.0, round(weighted_total))))

        if final_score >= 85:
            level = "S"
        elif final_score >= 70:
            level = "A"
        elif final_score >= 50:
            level = "B"
        else:
            level = "C"

        is_candidate = (final_score >= 70) and (not is_upbit_listed)

        breakdown = {
            "backer_score": round(backer_score, 1),
            "coinbase_score": round(coinbase_score, 1),
            "sector_score": round(sector_score, 1),
            "eco_score": round(eco_score, 1),
        }

        return {
            "token_symbol": sym_clean,
            "fit_score": final_score,
            "fit_level": level,
            "is_upbit_listed": is_upbit_listed,
            "is_coinbase_listed": is_coinbase_listed,
            "is_candidate": is_candidate,
            "matched_backers": matched_backers,
            "breakdown": breakdown,
        }
