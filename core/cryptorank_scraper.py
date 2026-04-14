"""
CryptoRank 融资数据爬虫

策略：CryptoRank v0/v1 API 已下线(404)，v2 需认证(401)。
改用 Next.js SSR API（_next/data/{buildId}/funding-rounds.json）获取首页 20 条，
再通过项目详情页补全融资金额、投资方等字段。
"""
import json
import logging
import re
import time
import httpx

logger = logging.getLogger("cryptorank-scraper")

_SITE_BASE = "https://cryptorank.io"


class CryptoRankScraper:
    """CryptoRank 融资数据爬虫。

    使用 Next.js SSR API + 项目详情页获取数据。
    """

    def __init__(self, proxy: str | None = None):
        self.proxy = proxy
        self._build_id: str | None = None

    def _get_client(self, timeout: float = 30) -> httpx.Client:
        return httpx.Client(
            proxy=self.proxy,
            timeout=timeout,
            verify=False,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/json,"
                          "text/plain,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )

    def _get_build_id(self, client: httpx.Client) -> str | None:
        """从首页 HTML 中提取 Next.js buildId。"""
        if self._build_id:
            return self._build_id

        try:
            resp = client.get(f"{_SITE_BASE}/funding-rounds")
            if resp.status_code != 200:
                logger.warning("[CryptoRank] 首页返回 %d", resp.status_code)
                return None

            html = resp.text

            # 提取 buildId
            m = re.search(r'"buildId":"([^"]+)"', html)
            if m:
                self._build_id = m.group(1)
                return self._build_id

            # 也尝试从 __NEXT_DATA__ 中提取
            nd = re.search(
                r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                html, re.DOTALL,
            )
            if nd:
                data = json.loads(nd.group(1))
                self._build_id = data.get("buildId")
                return self._build_id

        except Exception as e:
            logger.error("[CryptoRank] 获取 buildId 失败: %s", e)

        return None

    def _fetch_ssr_data(self, client: httpx.Client, build_id: str,
                        on_log=None) -> list[dict]:
        """通过 SSR API 获取 funding-rounds 数据。"""
        url = f"{_SITE_BASE}/_next/data/{build_id}/funding-rounds.json"
        if on_log:
            on_log(f"[CryptoRank] 使用 SSR API (buildId: {build_id[:10]}...)")

        try:
            resp = client.get(url)
            if resp.status_code != 200:
                logger.warning("[CryptoRank] SSR API 返回 %d", resp.status_code)
                return []

            data = resp.json()
            page_props = data.get("pageProps", {})
            rounds = page_props.get("fallbackRounds", {})
            items = rounds.get("data", [])
            total = rounds.get("total", 0)

            if on_log:
                on_log(f"[CryptoRank] SSR 返回 {len(items)} 条 (总计 {total})")

            return items
        except Exception as e:
            logger.error("[CryptoRank] SSR API 失败: %s", e)
            return []

    def _fetch_from_html(self, client: httpx.Client,
                         on_log=None) -> list[dict]:
        """Fallback: 从页面 HTML __NEXT_DATA__ 中提取数据。"""
        if on_log:
            on_log("[CryptoRank] 使用 HTML __NEXT_DATA__ 模式...")

        try:
            resp = client.get(f"{_SITE_BASE}/funding-rounds")
            if resp.status_code != 200:
                return []

            html = resp.text
            nd = re.search(
                r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                html, re.DOTALL,
            )
            if not nd:
                logger.warning("[CryptoRank] 页面中未找到 __NEXT_DATA__")
                return []

            data = json.loads(nd.group(1))
            rounds = (data.get("props", {})
                      .get("pageProps", {})
                      .get("fallbackRounds", {}))
            items = rounds.get("data", [])
            if on_log:
                on_log(f"[CryptoRank] HTML 解析到 {len(items)} 条")

            return items
        except Exception as e:
            logger.error("[CryptoRank] HTML 解析失败: %s", e)
            return []

    def _fetch_project_detail(self, client: httpx.Client, key: str,
                              on_log=None) -> dict | None:
        """获取单个项目详情（融资金额、投资方等）。

        使用 /price/{key} 页面 __NEXT_DATA__ 中的 coin 数据。
        """
        url = f"{_SITE_BASE}/price/{key}"
        try:
            resp = client.get(url)
            if resp.status_code != 200:
                return None

            html = resp.text
            nd = re.search(
                r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                html, re.DOTALL,
            )
            if not nd:
                return None

            data = json.loads(nd.group(1))
            pp = data.get("props", {}).get("pageProps", {})
            coin = pp.get("coin", {})
            if not coin:
                return None

            result = {}

            # 描述
            desc = coin.get("shortDescription") or coin.get("description", "")
            # 去除 HTML 标签
            desc = re.sub(r'<[^>]+>', '', desc)[:500]
            result["description"] = desc

            # 分类/标签
            category = coin.get("category", "")
            result["tags"] = json.dumps([category] if category else [],
                                       ensure_ascii=False)

            # 链接
            links = coin.get("links", [])
            for link in links:
                lt = link.get("type", "")
                lv = link.get("value", "")
                if lt == "web" and not result.get("website"):
                    result["website"] = lv
                elif lt == "twitter" and not result.get("twitter"):
                    result["twitter"] = lv

            # 融资信息 — 从 icoData.description 解析
            ico_data = coin.get("icoData", {})
            ico_desc = ico_data.get("description", "")
            # 尝试提取金额: "$44M", "$10M", "$1.5B"
            amount_match = re.search(
                r'\$(\d+(?:\.\d+)?)\s*(M|B|K)?',
                ico_desc, re.IGNORECASE,
            )
            if amount_match:
                amt = float(amount_match.group(1))
                unit = (amount_match.group(2) or "").upper()
                if unit == "B":
                    amt *= 1e9
                elif unit == "M":
                    amt *= 1e6
                elif unit == "K":
                    amt *= 1e3
                result["total_funding"] = amt

            # 轮次
            round_match = re.search(
                r'(Seed|Pre-Seed|Series\s+[A-D]|Strategic|Private|Public)',
                ico_desc, re.IGNORECASE,
            )
            if round_match:
                result["latest_round"] = round_match.group(1)

            # isTraded
            result["cr_traded"] = 1 if coin.get("isTraded") else 0

            # symbol
            if coin.get("symbol"):
                result["token_symbol"] = coin["symbol"]

            return result

        except Exception as e:
            logger.debug("[CryptoRank] 项目详情失败 %s: %s", key, e)
            return None

    def _normalize_item(self, item: dict) -> dict | None:
        """标准化 CryptoRank SSR 数据为统一格式。"""
        name = item.get("name")
        key = item.get("key", "")

        if not name:
            # 隐藏/排他性项目跳过
            if item.get("isHidden") or item.get("isExclusive"):
                return None
            return None

        return {
            "project_name": name,
            "logo": item.get("icon", ""),
            "description": "",
            "tags": "[]",
            "source": "cryptorank",
            "cryptorank_slug": key,
            "total_funding": None,
            "latest_round": "",
            "latest_round_date": item.get("date", ""),
            "investors": "[]",
            "token_symbol": item.get("symbol") or "",
            "cr_traded": 0,
            "website": "",
            "twitter": "",
            "_key": key,
        }

    def fetch_all_not_traded(self, max_pages: int = 20, page_size: int = 100,
                             on_log=None) -> list[dict]:
        """抓取所有融资项目。"""
        if on_log:
            on_log("[CryptoRank] 开始采集融资项目...")

        with self._get_client(timeout=60) as client:
            # 获取 buildId
            build_id = self._get_build_id(client)

            # 获取项目列表
            if build_id:
                raw_items = self._fetch_ssr_data(client, build_id, on_log)
            else:
                raw_items = self._fetch_from_html(client, on_log)

            if not raw_items:
                if on_log:
                    on_log("[CryptoRank] 未获取到数据")
                return []

            # 标准化
            projects = []
            for item in raw_items:
                proj = self._normalize_item(item)
                if proj:
                    projects.append(proj)

            if on_log:
                on_log(f"[CryptoRank] 有效项目 {len(projects)} 个，开始补全详情...")

            # 补全详情
            enriched = 0
            for proj in projects:
                key = proj.pop("_key", "")
                if key:
                    detail = self._fetch_project_detail(client, key, on_log)
                    if detail:
                        # 合并详情数据
                        for k, v in detail.items():
                            if v and (not proj.get(k) or proj.get(k) == "[]"):
                                proj[k] = v
                        enriched += 1
                    time.sleep(0.8)

            if on_log:
                on_log(f"[CryptoRank] 补全了 {enriched}/{len(projects)} 个项目的详情")

        return projects

    def fetch_funding_rounds(self, offset: int = 0, limit: int = 100,
                             is_traded: bool = False,
                             on_log=None) -> list[dict]:
        """兼容旧接口。"""
        return self.fetch_all_not_traded(on_log=on_log)
