"""
CMC API 资产核对模块

通过 CoinMarketCap API 验证代币是否已上所：
- /v1/cryptocurrency/map → symbol→id 映射
- /v2/cryptocurrency/market-pairs/latest → 交易对数量（0 = 未上所）
"""
import logging
import time
import httpx

logger = logging.getLogger("cmc-verifier")

_CMC_BASE = "https://pro-api.coinmarketcap.com"


class RateLimiter:
    """简单限速器，确保请求间隔。"""

    def __init__(self, rpm: int = 28):
        self._interval = 60.0 / rpm
        self._last = 0.0

    def wait(self):
        now = time.time()
        elapsed = now - self._last
        if elapsed < self._interval:
            time.sleep(self._interval - elapsed)
        self._last = time.time()


class CMCVerifier:
    """CMC API 资产核对。"""

    def __init__(self, api_key: str, proxy: str | None = None):
        self.api_key = api_key
        self.proxy = proxy
        self._limiter = RateLimiter(28)
        self._symbol_cache: dict[str, int] = {}

    def _get_client(self) -> httpx.Client:
        return httpx.Client(
            proxy=self.proxy,
            timeout=30,
            verify=False,
            headers={
                "X-CMC_PRO_API_KEY": self.api_key,
                "Accept": "application/json",
            },
        )

    def _request(self, method: str, path: str, **kwargs) -> dict | None:
        """带限速和重试的 API 请求。"""
        url = f"{_CMC_BASE}{path}"
        max_retries = 3

        for attempt in range(max_retries):
            self._limiter.wait()
            try:
                with self._get_client() as client:
                    resp = client.request(method, url, **kwargs)

                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", "60"))
                        logger.warning("[CMC] 429 限速，等待 %ds", retry_after)
                        time.sleep(retry_after)
                        continue

                    if resp.status_code >= 500:
                        wait = 2 ** attempt * 5
                        logger.warning("[CMC] %d 服务器错误，等待 %ds", resp.status_code, wait)
                        time.sleep(wait)
                        continue

                    resp.raise_for_status()
                    return resp.json()
            except httpx.TimeoutException:
                logger.warning("[CMC] 超时 (attempt %d/%d)", attempt + 1, max_retries)
                time.sleep(5)
            except Exception as e:
                logger.error("[CMC] 请求失败: %s", e)
                break

        return None

    def lookup_symbol(self, symbol: str) -> int | None:
        """通过 symbol 查找 CMC ID。"""
        if symbol in self._symbol_cache:
            return self._symbol_cache[symbol]

        data = self._request("GET", "/v1/cryptocurrency/map", params={
            "symbol": symbol.upper(),
            "limit": 5,
        })
        if not data:
            return None

        items = data.get("data", [])
        if not items:
            return None

        # 取第一个活跃的
        for item in items:
            if item.get("is_active") == 1:
                cmc_id = item["id"]
                self._symbol_cache[symbol] = cmc_id
                return cmc_id

        return items[0]["id"] if items else None

    def get_market_pairs(self, cmc_id: int) -> dict:
        """获取代币的交易对列表。

        返回 {num_market_pairs, exchanges: [...]}
        """
        data = self._request("GET", "/v2/cryptocurrency/market-pairs/latest", params={
            "id": cmc_id,
            "limit": 10,
        })
        if not data:
            return {"num_market_pairs": 0, "exchanges": []}

        crypto_data = data.get("data", {})
        num_pairs = crypto_data.get("num_market_pairs", 0)
        market_pairs = crypto_data.get("market_pairs", [])

        exchanges = list({
            mp.get("exchange", {}).get("name", "")
            for mp in market_pairs
            if mp.get("exchange", {}).get("name")
        })

        return {
            "num_market_pairs": num_pairs,
            "exchanges": exchanges,
        }

    def verify_token(self, symbol: str, on_log=None) -> dict:
        """验证单个代币的上所状态。

        返回:
            {
                symbol, cmc_id, cmc_listed (bool),
                num_market_pairs, exchanges: [...]
            }
        """
        if on_log:
            on_log(f"[CMC] 验证 {symbol}...")

        result = {
            "symbol": symbol,
            "cmc_id": None,
            "cmc_listed": False,
            "num_market_pairs": 0,
            "exchanges": [],
        }

        cmc_id = self.lookup_symbol(symbol)
        if not cmc_id:
            if on_log:
                on_log(f"[CMC] {symbol} 在 CMC 未找到")
            return result

        result["cmc_id"] = cmc_id

        pairs = self.get_market_pairs(cmc_id)
        result["num_market_pairs"] = pairs["num_market_pairs"]
        result["exchanges"] = pairs["exchanges"]
        result["cmc_listed"] = pairs["num_market_pairs"] > 0

        if on_log:
            status = f"{pairs['num_market_pairs']} 个交易对" if result["cmc_listed"] else "未上所"
            on_log(f"[CMC] {symbol} (ID={cmc_id}): {status}")

        return result

    def verify_batch(self, symbols: list[str],
                     on_log=None) -> list[dict]:
        """批量验证。"""
        results = []
        for i, sym in enumerate(symbols):
            if on_log:
                on_log(f"[CMC] [{i+1}/{len(symbols)}] 验证 {sym}")
            r = self.verify_token(sym, on_log)
            results.append(r)

        return results
