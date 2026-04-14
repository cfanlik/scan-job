"""
RootData 融资数据爬虫 (DrissionPage CDP 模式)

架构:
  DrissionPage 连接桌面 Chrome → 登录 cn.rootdata.com → DOM 提取融资列表 → 分页翻页
  登录状态由桌面 Chrome 保持，首次登录后后续免登录

数据流:
  1. CDP 检测登录态 → 未登录则自动登录
  2. 导航 /Fundraising → 解析渲染后的 DOM 表格
  3. 翻页采集 → 项目归一化输出
  4. 去重合并进 scanner 流程

采集频率: 一天一次
"""
import json
import logging
import os
import re
import time

logger = logging.getLogger("rootdata-cdp")

_SITE_BASE = "https://cn.rootdata.com"


class RootDataCDPScraper:
    """RootData DrissionPage CDP 爬虫。"""

    def __init__(self, email: str = "", password: str = ""):
        self.email = email or os.environ.get("ROOTDATA_EMAIL", "")
        self.password = password or os.environ.get("ROOTDATA_PASSWORD", "")
        self._page = None

    # ────────────────────────────────────────
    #  浏览器管理
    # ────────────────────────────────────────

    def _ensure_browser(self):
        if self._page is not None:
            return
        from DrissionPage import ChromiumPage, ChromiumOptions
        co = ChromiumOptions()
        co.set_argument('--no-first-run')
        self._page = ChromiumPage(addr_or_opts=co)

    def _check_login(self) -> bool:
        """登录页 URL 判断 + 按钮检测."""
        if '/login' in (self._page.url or ''):
            return False
        login_btn = self._page.ele('text:登录', timeout=2)
        # 排除导航栏上的"登录"链接（已登录时仍可能残留）
        if login_btn:
            href = login_btn.attr('href') or ''
            if '/login' in href:
                return False
        return True

    def _do_login(self, on_log=None) -> bool:
        if not self.email or not self.password:
            if on_log:
                on_log("[RootData] 未配置 ROOTDATA_EMAIL/PASSWORD")
            return False

        if on_log:
            on_log("[RootData] 执行登录...")

        self._page.get(f"{_SITE_BASE}/login")
        time.sleep(3)

        inputs = self._page.eles('tag:input')
        if len(inputs) < 2:
            if on_log:
                on_log("[RootData] 登录表单未找到")
            return False

        # input[0]=邮箱, input[1]=密码
        inputs[0].clear()
        inputs[0].input(self.email)
        time.sleep(0.3)
        inputs[1].clear()
        inputs[1].input(self.password)
        time.sleep(0.3)

        # 点击"登录"按钮
        for btn in self._page.eles('tag:button'):
            if btn.text and btn.text.strip() == '登录':
                btn.click()
                break
        time.sleep(5)

        success = '/login' not in (self._page.url or '')
        if on_log:
            on_log(f"[RootData] 登录{'成功' if success else '失败'}")
        return success

    def _dismiss_popups(self):
        """关闭常见弹窗."""
        try:
            for text in ['稍后', '稍后再说', '关闭', '我知道了']:
                btn = self._page.ele(f'text:{text}', timeout=0.5)
                if btn:
                    btn.click()
                    time.sleep(0.3)
        except Exception:
            pass

    # ────────────────────────────────────────
    #  数据提取
    # ────────────────────────────────────────

    def _parse_current_page(self) -> list[dict]:
        """从当前渲染的 DOM 表格 (<tr>) 提取融资项目列表.
        
        表格列结构 (从 DOM 分析):
          td[0]: 项目名 + 描述 + logo
          td[1]: 轮次 (种子/Pre-Seed/Series A 等)
          td[2]: 金额 (1500 万美元)
          td[3]: 估值
          td[4]: 日期 (04-10)
          td[5]: 投资方
        """
        projects = []
        rows = self._page.eles('css:table.b-table tbody tr')
        if not rows:
            rows = self._page.eles('css:tbody tr')

        for row in rows:
            try:
                tds = row.eles('tag:td')
                if len(tds) < 4:
                    continue

                td0 = tds[0]

                # ── td[0]: 项目名 + href + logo + 描述 ──
                # 结构: <a href="/Projects/detail/X"><img alt="X"></a>
                #        <div class="name"><span>X 描述</span><span>X</span></div>
                link = td0.ele('css:a[href*="/Projects/detail/"]', timeout=0.3)
                if not link:
                    continue
                href = link.attr('href') or ''
                if '/Projects/detail/' not in href:
                    continue

                # 项目名: div.name 内第二个 span（纯名称）
                name = ""
                desc = ""
                name_div = td0.ele('css:div.name', timeout=0.3)
                if name_div:
                    spans = name_div.eles('tag:span')
                    if len(spans) >= 2:
                        name = spans[-1].text.strip()  # 最后一个 span = 纯名称
                        desc = spans[0].text.strip()    # 第一个 span = 名称+描述
                    elif spans:
                        name = spans[0].text.strip()

                # 降级: 从 img alt 取名
                if not name:
                    img = link.ele('tag:img', timeout=0.2)
                    if img:
                        name = img.attr('alt') or ''

                if not name:
                    continue

                # 描述: 去掉项目名部分
                if desc and name in desc:
                    desc = desc.replace(name, '', 1).strip()

                # Logo
                logo = ""
                try:
                    img = td0.ele('tag:img', timeout=0.2)
                    if img:
                        logo = img.attr('src') or ""
                except Exception:
                    pass

                # ── td[1]: 轮次 ──
                round_text = tds[1].text.strip() if len(tds) > 1 else ""

                # ── td[2]: 金额 ──
                amount_text = tds[2].text.strip() if len(tds) > 2 else ""

                # ── td[4]: 日期 ──
                date_text = tds[4].text.strip() if len(tds) > 4 else ""

                # ── td[6]: 投资方 ──
                investors = []
                if len(tds) > 6:
                    inv_td = tds[6]
                    inv_links = inv_td.eles('tag:a')
                    if inv_links:
                        for il in inv_links:
                            t = il.text.strip()
                            if t and t != '--' and not t.startswith('+'):
                                investors.append(t)
                    elif inv_td.text.strip() and inv_td.text.strip() != '--':
                        # 降级: 纯文本按换行拆分
                        for part in inv_td.text.strip().replace('\n', ',').split(','):
                            part = part.strip()
                            if part and part != '--' and not part.startswith('+'):
                                investors.append(part)

                proj = {
                    "project_name": name,
                    "source": "rootdata",
                    "rootdata_url": href if href.startswith('http') else f"{_SITE_BASE}{href}",
                    "total_funding": self._parse_amount(amount_text),
                    "latest_round": self._clean_round(round_text),
                    "latest_round_date": self._parse_date(date_text),
                    "description": desc[:500],
                    "tags": "[]",
                    "investors": json.dumps(investors, ensure_ascii=False) if investors else "[]",
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
        if not text or text.strip() in ('--', 'N/A', ''):
            return None
        m = re.search(r'(\d+(?:\.\d+)?)\s*(亿|万|千万|百万|M|B)?', text)
        if not m:
            return None
        val = float(m.group(1))
        unit = m.group(2) or ""
        mul = {"亿": 1e8, "万": 1e4, "千万": 1e7, "百万": 1e6, "M": 1e6, "B": 1e9}.get(unit, 1)
        val *= mul
        return val if val >= 1000 else None

    @staticmethod
    def _clean_round(text: str) -> str:
        """清洗轮次: '种子轮', 'Pre-Seed', '--' → 归一化."""
        if not text or text.strip() in ('--', 'N/A', ''):
            return ""
        return text.strip()

    @staticmethod
    def _parse_date(text: str) -> str:
        """解析日期: '04-10', '2026-04-10' → 标准格式."""
        if not text or text.strip() in ('--', 'N/A', ''):
            return ""
        text = text.strip()
        # 补全年份
        if re.match(r'^\d{2}-\d{2}$', text):
            from datetime import datetime
            year = datetime.now().year
            return f"{year}-{text}"
        return text

    def _go_next_page(self, target: int) -> bool:
        """点击分页器翻到指定页."""
        try:
            # ElementUI 分页器: li.number
            pagers = self._page.eles('css:li.number, .el-pager li')
            for pg in pagers:
                if pg.text.strip() == str(target):
                    pg.click()
                    time.sleep(3)
                    return True

            # 备选: btn-next
            next_btn = self._page.ele('css:button.btn-next, li.btn-next', timeout=1)
            if next_btn:
                next_btn.click()
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
            on_log("[RootData] DrissionPage CDP 启动...")

        self._ensure_browser()

        # 导航到融资页
        self._page.get(f"{_SITE_BASE}/Fundraising")
        time.sleep(4)

        # 登录检测
        if not self._check_login():
            if on_log:
                on_log("[RootData] 未登录，尝试登录...")
            if not self._do_login(on_log):
                if on_log:
                    on_log("[RootData] 登录失败，尝试未登录采集")
            else:
                self._page.get(f"{_SITE_BASE}/Fundraising")
                time.sleep(4)

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
                on_log(f"[RootData] 第 {page_num}/{max_pages} 页: "
                       f"{len(projects)} 个项目 (累计 {len(all_projects)})")

            time.sleep(1.5)

        if on_log:
            on_log(f"[RootData] 共采集 {len(all_projects)} 个项目")

        return all_projects

    def close(self):
        """关闭浏览器连接（不关闭桌面 Chrome）."""
        self._page = None
