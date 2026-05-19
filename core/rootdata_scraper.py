"""
RootData 融资数据爬虫 (Camoufox 无头模式)

架构:
  Camoufox (隐身 Firefox) → 登录 cn.rootdata.com → DOM 提取融资列表 → 分页翻页
  移植 Scrapling 的 CF 盾牌点击逻辑绕过拦截
"""
import json
import logging
import math
import os
import re
import time
import random
from datetime import datetime
from typing import Callable, Optional

logger = logging.getLogger("rootdata-pw")

_SITE_BASE = "https://cn.rootdata.com"


class RootDataCDPScraper:
    """RootData Camoufox 无头爬虫（解决 Cloudflare）。"""

    def __init__(self, email: str = "", password: str = ""):
        self.email    = email    or os.environ.get("ROOTDATA_EMAIL", "")
        self.password = password or os.environ.get("ROOTDATA_PASSWORD", "")
        self._camoufox_ctx = None
        self._browser = None
        self._page    = None

    # ────────────────────────────────────────
    #  浏览器管理
    # ────────────────────────────────────────

    def _ensure_browser(self):
        if self._page is not None:
            return
        from camoufox.sync_api import Camoufox
        from config import get_proxy

        proxy_url = get_proxy()
        kwargs = {
            "headless": True,
            "viewport": {"width": 1440, "height": 900},
        }
        if proxy_url:
            kwargs["proxy"] = {"server": proxy_url}

        self._camoufox_ctx = Camoufox(**kwargs)
        self._browser = self._camoufox_ctx.__enter__()
        self._page = self._browser.new_page()

    def _solve_cloudflare(self) -> None:
        """移植 Scrapling 的 Cloudflare 自动识别与点击逻辑。"""
        if not self._page:
            return

        try:
            self._page.wait_for_timeout(3000)
            content = self._page.content()
            
            if "<title>Just a moment...</title>" not in content and "cf_turnstile" not in content and "cf-turnstile" not in content:
                return
                
            logger.info("[RootData] 侦测到 Cloudflare 拦截，尝试解决...")
            
            # 交互式验证检测
            if "Verifying you are human" in content or "cf-turnstile" in content:
                box_selector = "#cf_turnstile div, #cf-turnstile div, .turnstile>div>div"
                iframe = self._page.frame(url=re.compile(r"challenges\.cloudflare\.com"))
                
                outer_box = None
                if iframe is not None:
                    try:
                        self._page.wait_for_timeout(1000)
                        outer_box = iframe.frame_element().bounding_box()
                    except Exception:
                        pass
                
                if not outer_box:
                    try:
                        outer_box = self._page.locator(box_selector).last.bounding_box()
                    except Exception:
                        pass
                
                if outer_box:
                    captcha_x = outer_box["x"] + random.randint(20, 30)
                    captcha_y = outer_box["y"] + random.randint(20, 30)
                    self._page.mouse.click(captcha_x, captcha_y, delay=random.randint(100, 200), button="left")
                    logger.info("[RootData] 模拟点击 Cloudflare 盾牌...")
                    self._page.wait_for_timeout(3000)
            
            attempts = 0
            while "<title>Just a moment...</title>" in self._page.content() or "cf-turnstile" in self._page.content():
                if attempts >= 15:
                    logger.warning("[RootData] 等待 Cloudflare 消失超时")
                    break
                self._page.wait_for_timeout(1000)
                attempts += 1
                
            if "<title>Just a moment...</title>" not in self._page.content():
                logger.info("[RootData] Cloudflare 盾牌解除确认")
        except Exception as e:
            logger.warning(f"[RootData] Cloudflare 解决异常: {e}")

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

        self._page.goto(f"{_SITE_BASE}/login", wait_until="networkidle", timeout=45000)
        self._solve_cloudflare()
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
            self._page.wait_for_url(lambda url: "/login" not in url, timeout=15000)
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
        if on_log:
            on_log("[RootData] Camoufox 无头模式启动...")

        self._ensure_browser()

        self._page.goto(f"{_SITE_BASE}/Fundraising", wait_until="domcontentloaded", timeout=60000)
        self._solve_cloudflare()
        
        try:
            self._page.wait_for_selector("tbody tr", timeout=45000)
            if on_log:
                on_log("[RootData] 初次页面表格载入成功")
        except Exception as e:
            if on_log:
                on_log("[RootData] 等待初次表格加载超时，尝试继续执行")
            pass
            
        time.sleep(2)

        if not self._check_login():
            if on_log:
                on_log("[RootData] 未登录，尝试登录...")
            if not self._do_login(on_log):
                if on_log:
                    on_log("[RootData] 登录失败，尝试未登录采集")
            else:
                self._page.goto(
                    f"{_SITE_BASE}/Fundraising",
                    wait_until="domcontentloaded",
                    timeout=60000,
                )
                self._solve_cloudflare()
                try:
                    self._page.wait_for_selector("tbody tr", timeout=45000)
                except Exception:
                    pass
                time.sleep(2)

        self._dismiss_overlays()

        if on_log:
            on_log("[RootData] 应用高级筛选: Token Issuance -> With Token")
        try:
            sidebar = self._page.query_selector(".v-navigation-drawer--active, .left-filter, .filter-box")
            if sidebar:
                self._page.evaluate("""
                    document.querySelectorAll('label').forEach(l => {
                        if(l.innerText.includes('With Token') || l.textContent.includes('With Token')) {
                            l.click();
                        }
                    });
                """)
                time.sleep(5)
                try:
                    self._page.wait_for_selector("tbody tr", timeout=20000)
                except:
                    pass
        except Exception as e:
            logger.warning("[RootData] 点击 With Token 失败: %s", e)
            if on_log:
                on_log(f"[RootData] 点击 With Token 失败: {e}，将抓取全部")

        if max_pages == 0:
            total_pages = self.get_total_pages()
            if total_pages > 0:
                max_pages = total_pages
                if on_log:
                    on_log(f"[RootData] 自动检测总页数: {max_pages} 页")
            else:
                max_pages = 999
                if on_log:
                    on_log("[RootData] 总页数读取失败，将翻页至末页")

        all_projects = []
        consecutive_stop = 0

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
                    consecutive_stop = 0

            time.sleep(1.0)

        if on_log:
            on_log(f"[RootData] 共采集 {len(all_projects)} 个项目")

        return all_projects

    def close(self):
        """关闭浏览器与释放资源。"""
        try:
            if self._page:
                self._page.close()
            if self._browser:
                self._browser.close()
            if self._camoufox_ctx:
                self._camoufox_ctx.__exit__(None, None, None)
        except Exception:
            pass
        finally:
            self._page    = None
            self._browser = None
            self._camoufox_ctx = None
