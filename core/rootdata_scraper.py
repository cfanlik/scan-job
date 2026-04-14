"""
RootData 融资数据爬虫 (Playwright 无头模式)

架构:
  playwright Chromium 无头 → 登录 cn.rootdata.com → DOM 提取融资列表 → 分页翻页
  兼容 Linux VPS 无桌面环境

数据流:
  1. 启动无头 Chromium → 检测登录态 → 未登录则自动登录
  2. 导航 /Fundraising → 解析渲染后的 DOM 表格
  3. JS click 绕过遮罩 + btn-next + 内容变化检测翻页
  4. 支持 get_total_pages() 自动读取总页数
  5. 支持 early_stop_fn 每页回调，用于增量扫描提前终止

采集频率: 全量首次 / 增量每日一次
"""
import json
import logging
import math
import os
import re
import time
from datetime import datetime
from typing import Callable, Optional

logger = logging.getLogger("rootdata-pw")

_SITE_BASE = "https://cn.rootdata.com"


class RootDataCDPScraper:
    """RootData Playwright 无头爬虫（接口与原 DrissionPage 版保持一致）。"""

    def __init__(self, email: str = "", password: str = ""):
        self.email    = email    or os.environ.get("ROOTDATA_EMAIL", "")
        self.password = password or os.environ.get("ROOTDATA_PASSWORD", "")
        self._pw      = None
        self._browser = None
        self._context = None
        self._page    = None

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
        url = self._page.url or ""
        if "/login" in url:
            return False
        try:
            if self._page.query_selector("text=退出登录"):
                return True
            if self._page.query_selector("a[href*='/login']"):
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
            if btn.inner_text().strip() == "登录":
                btn.click()
                break

        try:
            self._page.wait_for_url(lambda url: "/login" not in url, timeout=10000)
        except Exception:
            pass

        time.sleep(2)
        success = "/login" not in (self._page.url or "")
        if on_log:
            on_log(f"[RootData] 登录{'成功' if success else '失败'}")
        return success

    def _dismiss_overlays(self):
        """JS 强制隐藏遮罩层，避免遮挡点击。"""
        try:
            self._page.evaluate("""
                document.querySelectorAll(
                    'div.bg[data-v-453a4645], .v-overlay, .v-dialog__overlay, ' +
                    '.modal-backdrop, .el-overlay'
                ).forEach(el => {
                    el.style.display = 'none';
                    el.style.pointerEvents = 'none';
                    el.style.visibility = 'hidden';
                });
            """)
        except Exception:
            pass
        for text in ["稍后", "稍后再说", "关闭", "我知道了"]:
            try:
                btn = self._page.query_selector(f"text={text}")
                if btn and btn.is_visible():
                    btn.evaluate("el => el.click()")
                    time.sleep(0.2)
            except Exception:
                pass

    # ────────────────────────────────────────
    #  分页信息
    # ────────────────────────────────────────

    def get_total_pages(self) -> int:
        """从分页器读取总条数并计算总页数。
        分页器 HTML: <span class="el-pagination__total">共 9495 条</span>
        """
        PER_PAGE = 30
        try:
            el = self._page.query_selector(".el-pagination__total")
            if el:
                text = el.inner_text()  # "共 9495 条"
                m = re.search(r"(\d+)", text.replace(",", ""))
                if m:
                    total_items = int(m.group(1))
                    pages = math.ceil(total_items / PER_PAGE)
                    logger.info("[RootData] 总条数=%d → 总页数=%d", total_items, pages)
                    return pages
        except Exception as e:
            logger.warning("[RootData] get_total_pages 失败: %s", e)
        return 0

    # ────────────────────────────────────────
    #  数据提取
    # ────────────────────────────────────────

    def _parse_current_page(self) -> list[dict]:
        """从当前渲染的 DOM 表格提取融资项目列表。

        表格列结构:
          td[0]: 项目名 + 描述 + logo
          td[1]: 轮次
          td[2]: 金额
          td[3]: 估值
          td[4]: 日期
          td[6]: 投资方
        """
        projects = []
        try:
            self._page.wait_for_selector("tbody tr", timeout=10000)
        except Exception:
            return projects

        for row in self._page.query_selector_all("tbody tr"):
            try:
                tds = row.query_selector_all("td")
                if len(tds) < 4:
                    continue

                td0  = tds[0]
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

                round_text  = tds[1].inner_text().strip() if len(tds) > 1 else ""
                amount_text = tds[2].inner_text().strip() if len(tds) > 2 else ""
                date_text   = tds[4].inner_text().strip() if len(tds) > 4 else ""

                investors = []
                if len(tds) > 6:
                    inv_td    = tds[6]
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

                full_url = href if href.startswith("http") else f"{_SITE_BASE}{href}"
                projects.append({
                    "project_name":      name,
                    "source":            "rootdata",
                    "rootdata_url":      full_url,
                    "total_funding":     self._parse_amount(amount_text),
                    "latest_round":      self._clean_round(round_text),
                    "latest_round_date": self._parse_date(date_text),
                    "description":       desc[:500],
                    "tags":              "[]",
                    "investors":         json.dumps(investors, ensure_ascii=False) if investors else "[]",
                    "token_symbol":      "",
                    "logo":              logo,
                })
            except Exception:
                continue
        return projects

    @staticmethod
    def _parse_amount(text: str):
        if not text or text.strip() in ("--", "N/A", ""):
            return None
        m = re.search(r"(\d+(?:\.\d+)?)\s*(亿|万|千万|百万|M|B)?", text)
        if not m:
            return None
        val  = float(m.group(1))
        unit = m.group(2) or ""
        mul  = {"亿": 1e8, "万": 1e4, "千万": 1e7, "百万": 1e6, "M": 1e6, "B": 1e9}.get(unit, 1)
        val *= mul
        return val if val >= 1000 else None

    @staticmethod
    def _clean_round(text: str) -> str:
        return text.strip() if text and text.strip() not in ("--", "N/A", "") else ""

    @staticmethod
    def _parse_date(text: str) -> str:
        if not text or text.strip() in ("--", "N/A", ""):
            return ""
        text = text.strip()
        if re.match(r"^\d{2}-\d{2}$", text):
            return f"{datetime.now().year}-{text}"
        return text

    # ────────────────────────────────────────
    #  翻页
    # ────────────────────────────────────────

    def _go_next_page(self, target: int) -> bool:
        """JS click 绕过遮罩点击 btn-next，内容变化检测确认翻页。"""
        try:
            self._dismiss_overlays()

            next_btn = self._page.query_selector(
                "button.btn-next, .el-pagination .btn-next"
            )
            if next_btn:
                if next_btn.get_attribute("disabled") is not None:
                    logger.debug("[RootData] btn-next disabled (last page)")
                    return False

                first_row_text = ""
                try:
                    rows = self._page.query_selector_all("tbody tr")
                    if rows:
                        first_row_text = rows[0].inner_text()[:40]
                except Exception:
                    pass

                next_btn.evaluate("el => el.click()")

                deadline = time.time() + 15
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

            # 备选：页码 li
            for pg in self._page.query_selector_all("li.number, .el-pager li"):
                if pg.inner_text().strip() == str(target):
                    pg.evaluate("el => el.click()")
                    time.sleep(3)
                    return True

        except Exception as e:
            logger.debug("[RootData] 翻页异常: %s", e)
        return False

    # ────────────────────────────────────────
    #  公开接口
    # ────────────────────────────────────────

    def fetch_all_pages(
        self,
        max_pages: int = 10,
        on_log=None,
        early_stop_fn: Optional[Callable[[list[dict]], bool]] = None,
    ) -> list[dict]:
        """采集融资项目列表（多页）。

        Args:
            max_pages:      最大采集页数；0 = 自动读取总页数（全量）
            on_log:         日志回调
            early_stop_fn:  每页采集后调用，返回 True 则停止翻页（用于增量 early stop）
        """
        if on_log:
            on_log("[RootData] Playwright 无头模式启动...")

        self._ensure_browser()

        self._page.goto(f"{_SITE_BASE}/Fundraising", wait_until="networkidle", timeout=30000)
        time.sleep(3)

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

        self._dismiss_overlays()

        # max_pages=0 → 自动读取总页数
        if max_pages == 0:
            total_pages = self.get_total_pages()
            if total_pages > 0:
                max_pages = total_pages
                if on_log:
                    on_log(f"[RootData] 自动检测总页数: {max_pages} 页")
            else:
                max_pages = 999  # fallback: 翻到 btn-next disabled 为止
                if on_log:
                    on_log("[RootData] 总页数读取失败，将翻页至末页")

        all_projects = []
        consecutive_stop = 0  # 连续触发 early stop 的页数计数

        for page_num in range(1, max_pages + 1):
            if page_num > 1:
                if not self._go_next_page(page_num):
                    if on_log:
                        on_log(f"[RootData] 翻到第 {page_num} 页失败，停止")
                    break
                self._dismiss_overlays()

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

            # early stop 检测（增量模式）
            if early_stop_fn is not None:
                if early_stop_fn(projects):
                    consecutive_stop += 1
                    if consecutive_stop >= 2:
                        if on_log:
                            on_log(f"[RootData] 连续 {consecutive_stop} 页触发 early stop，停止采集")
                        break
                    else:
                        if on_log:
                            on_log(f"[RootData] 第 {page_num} 页触发 early stop ({consecutive_stop}/2)，继续验证下一页")
                else:
                    consecutive_stop = 0  # 重置连续计数

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
            self._page    = None
            self._browser = None
            self._pw      = None
