"""
RootData 融资数据爬虫 (Playwright 无头模式)

架构:
  playwright Chromium 无头 → 登录 cn.rootdata.com → DOM 提取融资列表 → 分页翻页
  兼容 Linux VPS 无桌面环境（原 DrissionPage CDP 方案已废弃）

数据流:
  1. 启动无头 Chromium → 检测登录态 → 未登录则自动登录
  2. 导航 /Fundraising → 解析渲染后的 DOM 表格
  3. btn-next 逐页点击 + 内容变化检测翻页
  4. 去重合并进 scanner 流程

采集频率: 一天一次
"""
import json
import logging
import os
import re
import time
from datetime import datetime

logger = logging.getLogger("rootdata-pw")

_SITE_BASE = "https://cn.rootdata.com"


class RootDataCDPScraper:
    """RootData Playwright 无头爬虫（接口与原 DrissionPage 版保持一致）。"""

    def __init__(self, email: str = "", password: str = ""):
        self.email = email or os.environ.get("ROOTDATA_EMAIL", "")
        self.password = password or os.environ.get("ROOTDATA_PASSWORD", "")
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    # ────────────────────────────────────────
    #  浏览器管理
    # ────────────────────────────────────────

    def _ensure_browser(self):
        if self._page is not None:
            return
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-first-run",
                "--disable-extensions",
            ],
        )
        self._context = self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
            locale="zh-CN",
        )
        self._page = self._context.new_page()

    def _check_login(self) -> bool:
        """URL 判断 + 按钮检测."""
        url = self._page.url or ""
        if "/login" in url:
            return False
        try:
            logout = self._page.query_selector("text=退出登录")
            if logout:
                return True
            login_link = self._page.query_selector("a[href*='/login']")
            if login_link:
                return False
        except Exception:
            pass
        return True

    def _do_login(self, on_log=None) -> bool:
        if not self.email or not self.password:
            if on_log:
                on_log("[RootData] 未配置 ROOTDATA_EMAIL/PASSWORD")
            return False

        if on_log:
            on_log("[RootData] 执行登录...")

        self._page.goto(f"{_SITE_BASE}/login", wait_until="networkidle", timeout=30000)
        time.sleep(2)

        inputs = self._page.query_selector_all("input")
        if len(inputs) < 2:
            if on_log:
                on_log("[RootData] 登录表单未找到")
            return False

        inputs[0].fill(self.email)
        time.sleep(0.3)
        inputs[1].fill(self.password)
        time.sleep(0.3)

        for btn in self._page.query_selector_all("button"):
            txt = btn.inner_text().strip()
            if txt == "登录":
                btn.click()
                break

        try:
            self._page.wait_for_url(
                lambda url: "/login" not in url, timeout=10000
            )
        except Exception:
            pass

        time.sleep(2)
        success = "/login" not in (self._page.url or "")
        if on_log:
            on_log(f"[RootData] 登录{'成功' if success else '失败'}")
        return success

    def _dismiss_popups(self):
        """关闭常见弹窗."""
        for text in ["稍后", "稍后再说", "关闭", "我知道了"]:
            try:
                btn = self._page.query_selector(f"text={text}")
                if btn and btn.is_visible():
                    btn.click()
                    time.sleep(0.3)
            except Exception:
                pass

    # ────────────────────────────────────────
    #  数据提取
    # ────────────────────────────────────────

    def _parse_current_page(self) -> list[dict]:
        """从当前渲染的 DOM 表格提取融资项目列表.

        表格列结构:
          td[0]: 项目名 + 描述 + logo
          td[1]: 轮次
          td[2]: 金额
          td[3]: 估值
          td[4]: 日期
          td[5+]: 投资方
        """
        projects = []

        try:
            self._page.wait_for_selector("tbody tr", timeout=10000)
        except Exception:
            return projects

        rows = self._page.query_selector_all("tbody tr")

        for row in rows:
            try:
                tds = row.query_selector_all("td")
                if len(tds) < 4:
                    continue

                td0 = tds[0]

                # ── td[0]: 项目名 + href + logo ──
                link = td0.query_selector("a[href*='/Projects/detail/']")
                if not link:
                    continue
                href = link.get_attribute("href") or ""
                if "/Projects/detail/" not in href:
                    continue

                name = ""
                desc = ""
                name_div = td0.query_selector("div.name")
                if name_div:
                    spans = name_div.query_selector_all("span")
                    if len(spans) >= 2:
                        name = spans[-1].inner_text().strip()
                        desc = spans[0].inner_text().strip()
                    elif spans:
                        name = spans[0].inner_text().strip()

                if not name:
                    img = link.query_selector("img")
                    if img:
                        name = img.get_attribute("alt") or ""

                if not name:
                    continue

                if desc and name in desc:
                    desc = desc.replace(name, "", 1).strip()

                logo = ""
                try:
                    img = td0.query_selector("img")
                    if img:
                        logo = img.get_attribute("src") or ""
                except Exception:
                    pass

                # ── td[1]: 轮次 ──
                round_text = tds[1].inner_text().strip() if len(tds) > 1 else ""

                # ── td[2]: 金额 ──
                amount_text = tds[2].inner_text().strip() if len(tds) > 2 else ""

                # ── td[4]: 日期 ──
                date_text = tds[4].inner_text().strip() if len(tds) > 4 else ""

                # ── td[6]: 投资方 ──
                investors = []
                if len(tds) > 6:
                    inv_td = tds[6]
                    inv_links = inv_td.query_selector_all("a")
                    if inv_links:
                        for il in inv_links:
                            t = il.inner_text().strip()
                            if t and t != "--" and not t.startswith("+"):
                                investors.append(t)
                    else:
                        raw = inv_td.inner_text().strip()
                        if raw and raw != "--":
                            for part in raw.replace("\n", ",").split(","):
                                part = part.strip()
                                if part and part != "--" and not part.startswith("+"):
                                    investors.append(part)

                proj = {
                    "project_name": name,
                    "source": "rootdata",
                    "rootdata_url": (
                        href if href.startswith("http") else f"{_SITE_BASE}{href}"
                    ),
                    "total_funding": self._parse_amount(amount_text),
                    "latest_round": self._clean_round(round_text),
                    "latest_round_date": self._parse_date(date_text),
                    "description": desc[:500],
                    "tags": "[]",
                    "investors": (
                        json.dumps(investors, ensure_ascii=False) if investors else "[]"
                    ),
                    "token_symbol": "",
                    "logo": logo,
                }
                projects.append(proj)
            except Exception:
                continue

        return projects

    @staticmethod
    def _parse_amount(text: str):
        """解析金额: '1500 万美元', '1.2 亿美元', '$50M', '--'."""
        if not text or text.strip() in ("--", "N/A", ""):
            return None
        m = re.search(r"(\d+(?:\.\d+)?)\s*(亿|万|千万|百万|M|B)?", text)
        if not m:
            return None
        val = float(m.group(1))
        unit = m.group(2) or ""
        mul = {
            "亿": 1e8, "万": 1e4, "千万": 1e7, "百万": 1e6, "M": 1e6, "B": 1e9
        }.get(unit, 1)
        val *= mul
        return val if val >= 1000 else None

    @staticmethod
    def _clean_round(text: str) -> str:
        if not text or text.strip() in ("--", "N/A", ""):
            return ""
        return text.strip()

    @staticmethod
    def _parse_date(text: str) -> str:
        if not text or text.strip() in ("--", "N/A", ""):
            return ""
        text = text.strip()
        if re.match(r"^\d{2}-\d{2}$", text):
            year = datetime.now().year
            return f"{year}-{text}"
        return text

    def _go_next_page(self, target: int) -> bool:
        """点击 btn-next 翻到下一页，通过内容变化检测确认翻页成功。"""
        try:
            # 优先使用右箭头 btn-next 按钮
            next_btn = self._page.query_selector(
                "button.btn-next, .el-pagination .btn-next"
            )
            if next_btn:
                disabled = next_btn.get_attribute("disabled")
                if disabled is not None:
                    logger.debug("[RootData] btn-next 已禁用(最后一页)")
                    return False

                # 记录第一行内容，用于检测页面是否已刷新
                first_row_text = ""
                try:
                    rows = self._page.query_selector_all("tbody tr")
                    if rows:
                        first_row_text = rows[0].inner_text()[:40]
                except Exception:
                    pass

                next_btn.click()

                # 轮询等待内容变化（最多 12s）
                deadline = time.time() + 12
                while time.time() < deadline:
                    time.sleep(0.6)
                    try:
                        rows = self._page.query_selector_all("tbody tr")
                        if rows and rows[0].inner_text()[:40] != first_row_text:
                            break
                    except Exception:
                        pass
                time.sleep(0.5)
                return True

            # 备选：按页码 li 点击（跳转到特定页号）
            pagers = self._page.query_selector_all("li.number, .el-pager li")
            for pg in pagers:
                if pg.inner_text().strip() == str(target):
                    pg.click()
                    time.sleep(3)
                    return True

        except Exception as e:
            logger.debug("[RootData] 翻页异常: %s", e)
        return False

    # ────────────────────────────────────────
    #  公开接口
    # ────────────────────────────────────────

    def fetch_all_pages(self, max_pages: int = 10, on_log=None) -> list[dict]:
        """采集融资项目列表（多页）。"""
        if on_log:
            on_log("[RootData] Playwright 无头模式启动...")

        self._ensure_browser()

        # 导航到融资页
        self._page.goto(f"{_SITE_BASE}/Fundraising", wait_until="networkidle", timeout=30000)
        time.sleep(3)

        # 登录检测
        if not self._check_login():
            if on_log:
                on_log("[RootData] 未登录，尝试登录...")
            if not self._do_login(on_log):
                if on_log:
                    on_log("[RootData] 登录失败，尝试未登录采集")
            else:
                self._page.goto(
                    f"{_SITE_BASE}/Fundraising",
                    wait_until="networkidle",
                    timeout=30000,
                )
                time.sleep(3)

        self._dismiss_popups()

        all_projects = []
        for page_num in range(1, max_pages + 1):
            if page_num > 1:
                if not self._go_next_page(page_num):
                    if on_log:
                        on_log(f"[RootData] 翻到第 {page_num} 页失败，停止")
                    break
                self._dismiss_popups()

            projects = self._parse_current_page()
            if not projects:
                if on_log:
                    on_log(f"[RootData] 第 {page_num} 页无数据，停止")
                break

            all_projects.extend(projects)
            if on_log:
                on_log(
                    f"[RootData] 第 {page_num}/{max_pages} 页: "
                    f"{len(projects)} 个项目 (累计 {len(all_projects)})"
                )

            time.sleep(1.0)

        if on_log:
            on_log(f"[RootData] 共采集 {len(all_projects)} 个项目")

        return all_projects

    def close(self):
        """关闭浏览器。"""
        try:
            if self._browser:
                self._browser.close()
            if self._pw:
                self._pw.stop()
        except Exception:
            pass
        finally:
            self._page = None
            self._browser = None
            self._pw = None
