"""
RootData 详情页深度代币发现 (纯 CDP 模式)

为穿透高强度 WAF，放弃 HTTPX + API，采用纯无头浏览器。
通过拦截 图片/CSS 等无效资源加速，单页耗时控制在 3-5 秒。
执行周期：周级/月级（5337 项目 × 4s ≈ 6h）
"""
import logging
import os
import random
import time
from typing import Optional

logger = logging.getLogger("rootdata-detail")

class RootDataDetailScraper:
    """RootData 详情页 CDP 爬虫。"""

    def __init__(self, proxy: str | None = None):
        self.proxy = proxy

    def batch_scrape(self, projects: list[dict], on_log=None) -> dict:
        """批量深度匹配。

        Args:
            projects: [{id, project_name, rootdata_url}, ...]

        Returns:
            {found: [...], empty: int, failed: int, stats: {...}}
        """
        found = []
        empty_count = 0
        failed_count = 0
        total = len(projects)

        if on_log:
            on_log(f"[DeepDiscovery] 开始深度匹配: {total} 个项目 (纯 CDP 模式)")

        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()

        launch_args = ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        if self.proxy:
            launch_args.append(f"--proxy-server={self.proxy}")

        browser = None
        try:
            browser = pw.chromium.launch(headless=True, args=launch_args)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1440, "height": 900},
                locale="zh-CN",
            )

            for i, proj in enumerate(projects):
                pid = proj.get("id")
                name = proj.get("project_name", "")
                url = proj.get("rootdata_url", "")

                if not url:
                    failed_count += 1
                    continue

                result = None
                page = None
                try:
                    page = ctx.new_page()
                    # 拦截不必要的资源加速
                    page.route("**/*", lambda route: route.continue_() if route.request.resource_type in ["document", "script", "xhr", "fetch"] else route.abort())

                    page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    time.sleep(2) # 等待 __NUXT__ 赋给 window

                    nuxt = page.evaluate("() => window.__NUXT__")
                    if nuxt and isinstance(nuxt, dict):
                        data = nuxt.get("data", {})
                        for key, val in data.items():
                            if isinstance(val, dict) and "tokenSymbol" in val:
                                ts = (val.get("tokenSymbol") or "").strip()
                                tn = (val.get("tokenName") or "").strip()
                                result = {"tokenSymbol": ts, "tokenName": tn}
                                break

                    if not result:
                        # 尝试 dom 提取
                        symbol_el = page.query_selector(".symbol, .token-symbol, [class*='symbol']")
                        if symbol_el:
                            text = symbol_el.inner_text().strip()
                            if text and len(text) <= 10:
                                result = {"tokenSymbol": text, "tokenName": ""}
                                
                        if not result: # 如果没有 tokenSymbol 且没报错，说明就是无代币
                            result = {"tokenSymbol": "", "tokenName": ""}

                except Exception as e:
                    logger.warning("[DeepDiscovery] fetch error id=%s: %s", pid, e)
                finally:
                    if page:
                        page.close()

                if result and result.get("tokenSymbol"):
                    found.append({
                        "id": pid,
                        "project_name": name,
                        "token_symbol": result["tokenSymbol"],
                        "token_name": result.get("tokenName", ""),
                    })
                elif result is not None and not result.get("tokenSymbol"):
                    empty_count += 1 # 确认页面没有代币
                else:
                    failed_count += 1 # 加载抛出异常导致 result=None

                # 进度报告
                if on_log and (i + 1) % 50 == 0:
                    on_log(
                        f"[DeepDiscovery] 进度 {i+1}/{total}: "
                        f"发现 {len(found)}, 无代币 {empty_count}, 失败 {failed_count}"
                    )

                time.sleep(random.uniform(0.5, 1.5))

                # 重建 ctx 防止内存泄漏
                if (i + 1) % 200 == 0:
                    ctx.close()
                    ctx = browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0",
                        viewport={"width": 1440, "height": 900},
                        locale="zh-CN",
                    )

            stats = {
                "total": total,
                "found": len(found),
                "empty": empty_count,
                "failed": failed_count,
            }

            if on_log:
                on_log(f"[DeepDiscovery] 完成: 发现 {len(found)} 个代币, 无代币 {empty_count}, 失败 {failed_count}")

            return {"found": found, "stats": stats}

        finally:
            if browser:
                browser.close()
            pw.stop()
