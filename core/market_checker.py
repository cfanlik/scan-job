"""
现货 (Spot) 与 合约 (Futures / Derivatives) 主流市场核验引擎
数据源：
1. 优先尝试 CoinMarketCap Pro API (/v2/cryptocurrency/market-pairs/latest)
2. 自动聚合主流大所 (Binance, OKX, Bybit, Coinbase, Upbit) 官方公开免 Key API 进行精准兜底
"""
import time
import logging
import httpx
from datetime import datetime

logger = logging.getLogger("market-checker")


class MarketChecker:
    """主流交易所现货与合约市场核验器。"""

    def __init__(self, cmc_api_key: str = "", proxy: str | None = None):
        self.cmc_api_key = cmc_api_key
        self.proxy = proxy
        self._binance_spot_symbols: set[str] = set()
        self._binance_futures_symbols: set[str] = set()
        self._okx_spot_symbols: set[str] = set()
        self._okx_futures_symbols: set[str] = set()
        self._bybit_spot_symbols: set[str] = set()
        self._bybit_futures_symbols: set[str] = set()
        self._coinbase_symbols: set[str] = set()
        self._last_sync_time: float = 0.0
        self._cache: dict[str, dict] = {}

    def sync_exchange_universes(self, force: bool = False):
        """同步各主流大所的现货与合约标的池（免 Key 公开接口，缓存 2 小时）。"""
        now = time.time()
        if not force and (now - self._last_sync_time < 7200) and self._binance_spot_symbols:
            return

        client = httpx.Client(proxy=self.proxy, timeout=10.0, verify=False)
        headers = {"User-Agent": "Mozilla/5.0"}

        # 1. Binance 现货与合约
        try:
            r = client.get("https://api.binance.com/api/v3/exchangeInfo", headers=headers)
            if r.status_code == 200:
                data = r.json().get("symbols", [])
                self._binance_spot_symbols = {
                    s.get("baseAsset", "").upper()
                    for s in data
                    if s.get("status") == "TRADING" and s.get("quoteAsset") in ("USDT", "FDUSD", "USDC", "BTC")
                }
        except Exception as e:
            logger.warning("[MarketChecker] Binance Spot 同步失败: %s", e)

        try:
            r = client.get("https://fapi.binance.com/fapi/v1/exchangeInfo", headers=headers)
            if r.status_code == 200:
                data = r.json().get("symbols", [])
                self._binance_futures_symbols = {
                    s.get("baseAsset", "").upper()
                    for s in data
                    if s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL"
                }
        except Exception as e:
            logger.warning("[MarketChecker] Binance Futures 同步失败: %s", e)

        # 2. OKX 现货与合约
        try:
            r = client.get("https://www.okx.com/api/v5/public/instruments?instType=SPOT", headers=headers)
            if r.status_code == 200:
                data = r.json().get("data", [])
                self._okx_spot_symbols = {
                    item.get("baseCcy", "").upper()
                    for item in data
                    if item.get("state") == "live"
                }
        except Exception as e:
            logger.warning("[MarketChecker] OKX Spot 同步失败: %s", e)

        try:
            r = client.get("https://www.okx.com/api/v5/public/instruments?instType=SWAP", headers=headers)
            if r.status_code == 200:
                data = r.json().get("data", [])
                self._okx_futures_symbols = {
                    item.get("ctValCcy", "").upper()
                    for item in data
                    if item.get("state") == "live"
                }
        except Exception as e:
            logger.warning("[MarketChecker] OKX Swap 同步失败: %s", e)

        # 3. Bybit 现货与合约
        try:
            r = client.get("https://api.bybit.com/v5/market/instruments-info?category=spot", headers=headers)
            if r.status_code == 200:
                data = r.json().get("result", {}).get("list", [])
                self._bybit_spot_symbols = {
                    item.get("baseCoin", "").upper()
                    for item in data
                    if item.get("status") == "Trading"
                }
        except Exception as e:
            logger.warning("[MarketChecker] Bybit Spot 同步失败: %s", e)

        try:
            r = client.get("https://api.bybit.com/v5/market/instruments-info?category=linear", headers=headers)
            if r.status_code == 200:
                data = r.json().get("result", {}).get("list", [])
                self._bybit_futures_symbols = {
                    item.get("baseCoin", "").upper()
                    for item in data
                    if item.get("status") == "Trading"
                }
        except Exception as e:
            logger.warning("[MarketChecker] Bybit Linear 同步失败: %s", e)

        # 4. Coinbase 现货
        try:
            r = client.get("https://api.exchange.coinbase.com/currencies", headers=headers)
            if r.status_code == 200:
                data = r.json()
                self._coinbase_symbols = {
                    c.get("id", "").upper()
                    for c in data
                    if c.get("status") == "online"
                }
        except Exception as e:
            logger.warning("[MarketChecker] Coinbase 同步失败: %s", e)

        self._last_sync_time = now
        client.close()
        logger.info(
            "[MarketChecker] 交易所标的池同步完毕: "
            "Binance现货(%d)/合约(%d), OKX现货(%d)/合约(%d), Bybit现货(%d)/合约(%d), Coinbase(%d)",
            len(self._binance_spot_symbols), len(self._binance_futures_symbols),
            len(self._okx_spot_symbols), len(self._okx_futures_symbols),
            len(self._bybit_spot_symbols), len(self._bybit_futures_symbols),
            len(self._coinbase_symbols)
        )

    def check_market(self, symbol: str, is_upbit_listed: bool = False) -> dict:
        """
        全面检测代币在主流现货与合约交易所的上线状态。
        """
        sym = (symbol or "").strip().upper()
        if not sym:
            return {
                "has_spot": 0, "has_futures": 0,
                "spot_exchanges": [], "futures_exchanges": [],
                "coinbase_listed": 0, "market_status": "DEX_ONLY"
            }

        if sym in self._cache:
            return self._cache[sym]

        self.sync_exchange_universes()

        spot_exchanges = []
        futures_exchanges = []

        # 现货检测
        if sym in self._binance_spot_symbols:
            spot_exchanges.append("Binance")
        if sym in self._okx_spot_symbols:
            spot_exchanges.append("OKX")
        if sym in self._bybit_spot_symbols:
            spot_exchanges.append("Bybit")
        if sym in self._coinbase_symbols:
            spot_exchanges.append("Coinbase")
        if is_upbit_listed:
            spot_exchanges.append("Upbit")

        # 合约检测
        if sym in self._binance_futures_symbols:
            futures_exchanges.append("Binance Futures")
        if sym in self._okx_futures_symbols:
            futures_exchanges.append("OKX Swap")
        if sym in self._bybit_futures_symbols:
            futures_exchanges.append("Bybit Linear")

        has_spot = 1 if len(spot_exchanges) > 0 else 0
        has_futures = 1 if len(futures_exchanges) > 0 else 0
        coinbase_listed = 1 if "Coinbase" in spot_exchanges else 0

        if has_spot and has_futures:
            market_status = "BOTH"
        elif has_spot:
            market_status = "SPOT_ONLY"
        elif has_futures:
            market_status = "FUTURES_ONLY"
        else:
            market_status = "DEX_ONLY"

        res = {
            "symbol": sym,
            "has_spot": has_spot,
            "has_futures": has_futures,
            "spot_exchanges": spot_exchanges,
            "futures_exchanges": futures_exchanges,
            "coinbase_listed": coinbase_listed,
            "market_status": market_status,
        }
        self._cache[sym] = res
        return res
