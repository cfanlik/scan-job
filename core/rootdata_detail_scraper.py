"""
RootData 详情页深度代币发现

策略：
  1. Playwright CDP 登录 RootData → 获取 session cookie
  2. httpx + cookie 批量调用 getProjectDetail API
  3. 提取 tokenSymbol → 入库
  4. WAF 拦截时回退纯 CDP

执行周期：周级/月级（5337 项目 × 4s ≈ 6h）
"""
import base64
import json
import logging
import os
import random
import re
import time
import urllib.parse
from typing import Optional

import httpx

logger = logging.getLogger("rootdata-detail")

_API_BASE = "https://www.rootdata.com/pc/Api/getProjectDetail"
_SITE_BASE = "https://www.rootdata.com"
_CN_SITE = "https://cn.rootdata.com"


class RootDataDetailScraper:
    """RootData 详情页 API 批量爬虫。"""

    def __init__(self, email: str = "", password: str = "", proxy: str | None = None):
        self.email = email or os.environ.get("ROOTDATA_EMAIL", "")
        self.password = password or os.environ.get("ROOTDATA_PASSWORD", "")
        self.proxy = proxy
        self._cookies: dict = {}
        self._cookie_age = 0

    # ────────────────────────────────────────
    #  从 URL 提取 rootdata_id
    # ────────────────────────────────────────

    @staticmethod
    def extract_rootdata_id(rootdata_url: str) -> Optional[int]:
        """从 rootdata_url 的 k 参数 base64 解码出数字 ID。"""
        if not rootdata_url:
            return None
        try:
            parsed = urllib.parse.urlparse(rootdata_url)
            params = urllib.parse.parse_qs(parsed.query)
            k_val = params.get("k", [""])[0]
            if not k_val:
                return None
            decoded = base64.b64decode(k_val).decode("utf-8")
            return int(decoded)
        except Exception:
            return None

    # ────────────────────────────────────────
    #  CDP 登录获取 cookie
    # ────────────────────────────────────────

    def _login_cdp(self, on_log=None) -> dict:
        """Playwright CDP 登录 RootData，返回 cookie dict。"""
        from playwright.sync_api import sync_playwright

        if on_log:
            on_log("[DeepDiscovery] CDP 登录 RootData...")

        cookies = {}
        pw = sync_playwright().start()
        try:
            launch_args = ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            if self.proxy:
                launch_args.append(f"--proxy-server={self.proxy}")
            
            browser = pw.chromium.launch(
                headless=True,
                args=launch_args,
            )
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1440, "height": 900},
                locale="zh-CN",
            )
            page = ctx.new_page()

            # 访问登录页
            page.goto(f"{_CN_SITE}/login", wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            # 输入邮箱密码
            email_input = page.query_selector("input[type='text'], input[placeholder*='邮箱'], input[placeholder*='email']")
            pwd_input = page.query_selector("input[type='password']")

            if email_input and pwd_input:
                email_input.fill(self.email)
                pwd_input.fill(self.password)
                time.sleep(1)

                # 点击登录按钮
                login_btn = page.query_selector("button[type='submit'], button:has-text('登录'), .login-btn")
                if login_btn:
                    login_btn.click()
                    time.sleep(5)

            # 提取 cookies
            for c in ctx.cookies():
                cookies[c["name"]] = c["value"]

            if on_log:
                on_log(f"[DeepDiscovery] 获取 {len(cookies)} 个 cookie")

            browser.close()
        except Exception as e:
            if on_log:
                on_log(f"[DeepDiscovery] CDP 登录失败: {e}")
            logger.warning("[DeepDiscovery] CDP 登录失败: %s", e)
        finally:
            pw.stop()

        return cookies

    def _ensure_cookies(self, on_log=None):
        """确保 cookie 有效（每 500 次刷新）。"""
        if not self._cookies or (time.time() - self._cookie_age > 1800):
            self._cookies = self._login_cdp(on_log)
            self._cookie_age = time.time()

    # ────────────────────────────────────────
    #  httpx 调用 API
    # ────────────────────────────────────────

    def _fetch_detail(self, rootdata_id: int) -> Optional[dict]:
        """调用 getProjectDetail API，返回 {tokenSymbol, ...} 或 None。"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"{_SITE_BASE}/Projects/detail/Project?k=",
        }

        for attempt in range(3):
            try:
                with httpx.Client(
                    proxy=self.proxy, timeout=15, verify=False,
                    headers=headers, cookies=self._cookies,
                ) as client:
                    resp = client.get(f"{_API_BASE}?id={rootdata_id}")

                    if resp.status_code == 524 or resp.status_code == 403:
                        # WAF 拦截，等待重试
                        wait = (attempt + 1) * 10
                        logger.warning("[DeepDiscovery] WAF %d for id=%d, wait %ds", resp.status_code, rootdata_id, wait)
                        time.sleep(wait)
                        continue

                    if resp.status_code != 200:
                        return None

                    data = resp.json()
                    if data.get("code") != 200 and data.get("code") != 0:
                        return None

                    detail = data.get("data", {})
                    if isinstance(detail, dict) and "detail" in detail:
                        detail = detail["detail"]

                    return {
                        "tokenSymbol": (detail.get("tokenSymbol") or "").strip(),
                        "tokenName": (detail.get("tokenName") or detail.get("name") or "").strip(),
                        "contracts": detail.get("contracts", []),
                    }

            except (json.JSONDecodeError, httpx.HTTPError) as e:
                if attempt < 2:
                    time.sleep((attempt + 1) * 5)
                    continue
                return None
            except Exception:
                return None

        return None

    # ────────────────────────────────────────
    #  CDP 回退
    # ────────────────────────────────────────

    def _cdp_fallback(self, rootdata_url: str) -> Optional[dict]:
        """纯 CDP 方式获取 tokenSymbol（回退用）。"""
        from playwright.sync_api import sync_playwright

        pw = sync_playwright().start()
        try:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0",
                viewport={"width": 1440, "height": 900},
            )
            page = ctx.new_page()
            page.goto(rootdata_url, wait_until="networkidle", timeout=30000)
            time.sleep(3)

            # 从 __NUXT__ 提取
            nuxt = page.evaluate("() => window.__NUXT__")
            if nuxt and isinstance(nuxt, dict):
                data = nuxt.get("data", {})
                for key, val in data.items():
                    if isinstance(val, dict) and "tokenSymbol" in val:
                        return {
                            "tokenSymbol": (val.get("tokenSymbol") or "").strip(),
                            "tokenName": (val.get("tokenName") or "").strip(),
                        }

            # DOM 提取
            symbol_el = page.query_selector(".symbol, .token-symbol, [class*='symbol']")
            if symbol_el:
                text = symbol_el.inner_text().strip()
                if text and len(text) <= 10:
                    return {"tokenSymbol": text, "tokenName": ""}

            browser.close()
        except Exception:
            pass
        finally:
            pw.stop()

        return None

    # ────────────────────────────────────────
    #  批量处理
    # ────────────────────────────────────────

    def batch_scrape(self, projects: list[dict], on_log=None) -> dict:
        """批量深度匹配。

        Args:
            projects: [{id, project_name, rootdata_url}, ...]

        Returns:
            {found: [...], empty: int, failed: int, stats: {...}}
        """
        self._ensure_cookies(on_log)

        found = []
        empty_count = 0
        failed_count = 0
        waf_count = 0
        total = len(projects)

        if on_log:
            on_log(f"[DeepDiscovery] 开始深度匹配: {total} 个项目")

        for i, proj in enumerate(projects):
            pid = proj.get("id")
            name = proj.get("project_name", "")
            url = proj.get("rootdata_url", "")

            rootdata_id = self.extract_rootdata_id(url)
            if not rootdata_id:
                failed_count += 1
                continue

            result = self._fetch_detail(rootdata_id)

            if result is None:
                failed_count += 1
                waf_count += 1
                # 连续 WAF 失败时刷新 cookie
                if waf_count % 10 == 0:
                    if on_log:
                        on_log(f"[DeepDiscovery] 连续 WAF，刷新 cookie...")
                    self._cookies = self._login_cdp(on_log)
                    self._cookie_age = time.time()
            elif result["tokenSymbol"]:
                found.append({
                    "id": pid,
                    "project_name": name,
                    "token_symbol": result["tokenSymbol"],
                    "token_name": result.get("tokenName", ""),
                })
                waf_count = 0
            else:
                empty_count += 1
                waf_count = 0

            # 进度报告
            if on_log and (i + 1) % 100 == 0:
                on_log(
                    f"[DeepDiscovery] 进度 {i+1}/{total}: "
                    f"发现 {len(found)}, 无代币 {empty_count}, 失败 {failed_count}"
                )

            # 随机延迟
            time.sleep(random.uniform(2.5, 4.5))

            # 每 500 次刷新 cookie
            if (i + 1) % 500 == 0:
                self._ensure_cookies(on_log)

        stats = {
            "total": total,
            "found": len(found),
            "empty": empty_count,
            "failed": failed_count,
        }

        if on_log:
            on_log(f"[DeepDiscovery] 完成: 发现 {len(found)} 个代币, 无代币 {empty_count}, 失败 {failed_count}")

        return {"found": found, "stats": stats}
