"""
CryptoRank API v3 专用客户端
借鉴 select-coin GMGN 轮询与限流机制：
1. 多 Key 轮询负载均衡 (Least-Used 调度)
2. 严格令牌桶节流 (10 req/min，请求间隔 6.0s)
3. 429 与 5xx 指数退避重试 (带 Jitter)
4. 静态元数据本地持久化快照缓存 (7 天有效期，极大节省 Credits)
"""
import os
import time
import json
import random
import logging
import sqlite3
import httpx
from datetime import datetime

logger = logging.getLogger("cryptorank-client")

_CORE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_CORE_DIR)
_CACHE_DB = os.path.join(_PROJECT_ROOT, "data", "cryptorank_cache.db")


class CryptoRankClient:
    """CryptoRank API v3 客户端。"""

    def __init__(self, api_base: str = "https://api.cryptorank.io/v3",
                 rate_limit_per_min: int = 10,
                 proxy: str | None = None):
        self.api_base = api_base.rstrip("/")
        self.rate_limit_per_min = rate_limit_per_min
        self.min_interval = 60.0 / max(1, rate_limit_per_min)
        self.proxy = proxy
        self._last_request_time = 0.0
        self._init_cache_db()

    def _init_cache_db(self):
        os.makedirs(os.path.dirname(_CACHE_DB), exist_ok=True)
        with sqlite3.connect(_CACHE_DB) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_cache (
                    endpoint TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
            """)
            conn.commit()

    def _get_cache(self, endpoint: str, max_age_seconds: float = 86400 * 7) -> dict | list | None:
        try:
            with sqlite3.connect(_CACHE_DB) as conn:
                row = conn.execute(
                    "SELECT data_json, updated_at FROM api_cache WHERE endpoint = ?",
                    (endpoint,)
                ).fetchone()
                if row:
                    data_json, updated_at = row
                    if (time.time() - updated_at) < max_age_seconds:
                        return json.loads(data_json)
        except Exception as e:
            logger.warning("[CryptoRank Cache] 读取缓存异常: %s", e)
        return None

    def _set_cache(self, endpoint: str, data: dict | list):
        try:
            with sqlite3.connect(_CACHE_DB) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO api_cache (endpoint, data_json, updated_at) VALUES (?, ?, ?)",
                    (endpoint, json.dumps(data, ensure_ascii=False), time.time())
                )
                conn.commit()
        except Exception as e:
            logger.warning("[CryptoRank Cache] 写入缓存异常: %s", e)

    def _throttle(self):
        """令牌桶与节流器：确保两次请求间隔不低于 min_interval。"""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.min_interval:
            wait_time = self.min_interval - elapsed
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def request(self, path: str, params: dict | None = None,
                use_cache: bool = False, max_cache_age: float = 86400 * 7,
                max_retries: int = 3) -> dict | list | None:
        """核心请求方法，集成限流、退避重试与多 Key 分配。"""
        endpoint_key = f"{path}?{json.dumps(params, sort_keys=True)}" if params else path

        if use_cache:
            cached = self._get_cache(endpoint_key, max_cache_age)
            if cached is not None:
                return cached

        import config
        url = f"{self.api_base}{path}"

        for attempt in range(max_retries):
            key = config.acquire_cryptorank_key()
            if not key:
                logger.error("[CryptoRank] 未配置可用 API Key")
                return None

            self._throttle()

            headers = {
                "X-Api-Key": key,
                "Accept": "application/json",
                "User-Agent": "scan-job/2.0 (Institutional Collector)"
            }

            try:
                with httpx.Client(proxy=self.proxy, timeout=30.0, verify=False) as client:
                    resp = client.get(url, headers=headers, params=params)

                if resp.status_code == 200:
                    data = resp.json().get("data", [])
                    if use_cache:
                        self._set_cache(endpoint_key, data)
                    return data
                elif resp.status_code == 429:
                    backoff = min(60.0, (2.0 ** attempt) * 2.0 + random.uniform(0.5, 1.5))
                    logger.warning("[CryptoRank] 触发 429 限流，等待 %.1fs 后重试 (第 %d 次)", backoff, attempt + 1)
                    time.sleep(backoff)
                elif resp.status_code == 403:
                    body = resp.text
                    if "ENDPOINT_NOT_AVAILABLE" in body or "tariff plan" in body:
                        logger.warning("[CryptoRank] 端点 %s 在当前套餐不可用 (403 Forbidden)", path)
                        return None
                    logger.error("[CryptoRank] 403 错误: %s", body[:200])
                    return None
                elif resp.status_code >= 500:
                    backoff = (2.0 ** attempt) * 1.5 + random.uniform(0.2, 0.8)
                    logger.warning("[CryptoRank] 服务端错误 %d，等待 %.1fs 后重试", resp.status_code, backoff)
                    time.sleep(backoff)
                else:
                    logger.error("[CryptoRank] 请求失败 %s -> HTTP %d: %s", path, resp.status_code, resp.text[:200])
                    return None
            except Exception as e:
                logger.warning("[CryptoRank] 网络异常: %s，重试中...", e)
                time.sleep(2.0)
            finally:
                config.release_cryptorank_key(key)

        return None

    # ---------- 业务专用查询端点 ---------- #

    def get_currencies_map(self) -> list[dict]:
        """获取全量代币基础字典 (3.9万条，本地持久化 7 天)。"""
        res = self.request("/currencies/map", use_cache=True, max_cache_age=86400 * 7)
        return res if isinstance(res, list) else []

    def get_funds_map(self) -> list[dict]:
        """获取全量机构字典 (1.2万条，本地持久化 7 天)。"""
        res = self.request("/funds/map", use_cache=True, max_cache_age=86400 * 7)
        return res if isinstance(res, list) else []

    def get_exchanges_map(self) -> list[dict]:
        """获取全量交易所字典 (295个，本地持久化 7 天)。"""
        res = self.request("/exchanges/map", use_cache=True, max_cache_age=86400 * 7)
        return res if isinstance(res, list) else []

    def get_drophunting_map(self) -> list[dict]:
        """获取空投与早期一级项目跟踪列表 (1143个)。"""
        res = self.request("/drophunting/map", use_cache=True, max_cache_age=86400 * 3)
        return res if isinstance(res, list) else []

    def get_currency_detail(self, currency_id: int) -> dict | None:
        """获取单一代币详情（生命周期、市值、链接、标签）。"""
        res = self.request(f"/currencies/{currency_id}", use_cache=True, max_cache_age=86400 * 1)
        return res if isinstance(res, dict) else None

    def get_funding_rounds_feed(self, limit: int = 100) -> list[dict]:
        """获取融资事件流（Pro/Advanced 权限，若 Sandbox 则优雅降级返回空列表）。"""
        res = self.request("/funding-rounds/list", params={"limit": limit})
        return res if isinstance(res, list) else []
