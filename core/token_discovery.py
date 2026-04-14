"""
CMC 全量 Map 代币发现引擎

策略：
  1. 一次性下载 CMC /v1/cryptocurrency/map 全部活跃币种（~8400+）
  2. 构建 name→symbol、slug→symbol 双索引（内存 ~4MB）
  3. 对每个项目名执行三层本地匹配：精确 name → slug → fuzzy 子串
  4. 匹配完成后丢弃索引

性能：2-3 次 API 调用 + 纯内存匹配，6100 个项目 <1 分钟
"""
import logging
import re
import time
from typing import Optional

import httpx

logger = logging.getLogger("token-discovery")


class TokenDiscovery:
    """CMC 全量 Map 本地匹配引擎。"""

    def __init__(self, cmc_api_key: str, proxy: str | None = None):
        self.cmc_api_key = cmc_api_key
        self.proxy = proxy
        self._name_map: dict[str, list[dict]] = {}
        self._slug_map: dict[str, dict] = {}
        self._loaded = False

    # ────────────────────────────────────────
    #  CMC Map 下载
    # ────────────────────────────────────────

    def load_cmc_map(self, on_log=None) -> int:
        """下载全量 CMC map 构建内存索引，返回总币种数。"""
        if self._loaded:
            total = sum(len(v) for v in self._name_map.values())
            if on_log:
                on_log(f"[Discovery] CMC map 已加载 ({total} 币种)")
            return total

        headers = {"X-CMC_PRO_API_KEY": self.cmc_api_key, "Accept": "application/json"}
        all_coins: list[dict] = []

        for start in [1, 5001, 10001, 15001]:
            try:
                with httpx.Client(
                    proxy=self.proxy, timeout=30, verify=False, headers=headers
                ) as client:
                    resp = client.get(
                        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map",
                        params={
                            "listing_status": "active",
                            "start": start,
                            "limit": 5000,
                            "sort": "id",
                        },
                    )
                    data = resp.json()
                    items = data.get("data", [])
                    all_coins.extend(items)
                    if on_log:
                        on_log(f"[Discovery] CMC map batch start={start}: {len(items)} 币种")
                    if len(items) < 5000:
                        break
                    time.sleep(1)
            except Exception as e:
                logger.warning("[Discovery] CMC map 下载失败 start=%d: %s", start, e)
                if on_log:
                    on_log(f"[Discovery] CMC map 下载失败 start={start}: {e}")
                break

        # 构建索引
        self._name_map.clear()
        self._slug_map.clear()

        for c in all_coins:
            name = c.get("name", "").strip()
            symbol = c.get("symbol", "").strip()
            slug = c.get("slug", "").strip()
            cmc_id = c.get("id", 0)
            is_active = c.get("is_active", 0)

            if not name or not symbol or not is_active:
                continue

            entry = {"symbol": symbol, "slug": slug, "id": cmc_id, "name": name}

            key = name.lower()
            if key not in self._name_map:
                self._name_map[key] = []
            self._name_map[key].append(entry)

            if slug:
                self._slug_map[slug] = entry

        self._loaded = True
        total = len(all_coins)
        if on_log:
            on_log(
                f"[Discovery] CMC map 加载完成: {total} 币种, "
                f"{len(self._name_map)} 唯一名称, {len(self._slug_map)} slug"
            )
        return total

    # ────────────────────────────────────────
    #  三层匹配
    # ────────────────────────────────────────

    @staticmethod
    def _slugify(name: str) -> str:
        """项目名 → CMC slug 格式。"""
        s = name.strip().lower()
        s = re.sub(r"[^a-z0-9\s-]", "", s)
        s = re.sub(r"\s+", "-", s)
        return s

    def match_project(self, project_name: str) -> Optional[dict]:
        """三层匹配单个项目名。

        返回 {symbol, cmc_name, cmc_id, match_method} 或 None。
        """
        if not self._loaded:
            return None

        name = project_name.strip()
        key = name.lower()

        # L1: name 精确匹配
        if key in self._name_map:
            e = self._name_map[key][0]
            return {
                "symbol": e["symbol"],
                "cmc_name": e["name"],
                "cmc_id": e["id"],
                "match_method": "name_exact",
            }

        # L2: slug 匹配
        slug = self._slugify(name)
        if slug in self._slug_map:
            e = self._slug_map[slug]
            return {
                "symbol": e["symbol"],
                "cmc_name": e["name"],
                "cmc_id": e["id"],
                "match_method": "slug",
            }

        # L2.5: slug 变体（加/去常见后缀）
        for suffix in ["-protocol", "-network", "-finance", "-labs", "-dao", "-token"]:
            variant = slug + suffix
            if variant in self._slug_map:
                e = self._slug_map[variant]
                return {
                    "symbol": e["symbol"],
                    "cmc_name": e["name"],
                    "cmc_id": e["id"],
                    "match_method": "slug_variant",
                }
            if slug.endswith(suffix):
                stripped = slug[: -len(suffix)]
                if stripped in self._slug_map:
                    e = self._slug_map[stripped]
                    return {
                        "symbol": e["symbol"],
                        "cmc_name": e["name"],
                        "cmc_id": e["id"],
                        "match_method": "slug_stripped",
                    }

        # L3: fuzzy 子串匹配（项目名 ≥ 4 字符）
        if len(key) >= 4:
            for cmc_name, entries in self._name_map.items():
                if len(cmc_name) >= 4 and (key in cmc_name or cmc_name in key):
                    e = entries[0]
                    return {
                        "symbol": e["symbol"],
                        "cmc_name": e["name"],
                        "cmc_id": e["id"],
                        "match_method": "fuzzy",
                    }

        return None

    # ────────────────────────────────────────
    #  批量发现
    # ────────────────────────────────────────

    def batch_discover(self, projects: list[dict], on_log=None) -> dict:
        """批量匹配项目列表。

        Args:
            projects: [{id, project_name}, ...]

        Returns:
            {matched: [...], unmatched: [...], stats: {...}}
        """
        if not self._loaded:
            self.load_cmc_map(on_log)

        matched = []
        unmatched = []
        by_method: dict[str, int] = {}

        for i, proj in enumerate(projects):
            pid = proj.get("id")
            name = proj.get("project_name", "")

            result = self.match_project(name)
            if result:
                matched.append({"id": pid, "project_name": name, **result})
                method = result["match_method"]
                by_method[method] = by_method.get(method, 0) + 1
            else:
                unmatched.append({"id": pid, "project_name": name})

            if on_log and (i + 1) % 1000 == 0:
                on_log(
                    f"[Discovery] 进度 {i+1}/{len(projects)}: "
                    f"匹配 {len(matched)}, 未匹配 {len(unmatched)}"
                )

        stats = {
            "total": len(projects),
            "matched": len(matched),
            "unmatched": len(unmatched),
            "by_method": by_method,
        }

        if on_log:
            pct = stats["matched"] / stats["total"] * 100 if stats["total"] else 0
            on_log(f"[Discovery] 完成: {stats['matched']}/{stats['total']} 匹配 ({pct:.1f}%)")
            for method, count in by_method.items():
                on_log(f"[Discovery]   {method}: {count}")

        return {"matched": matched, "unmatched": unmatched, "stats": stats}
