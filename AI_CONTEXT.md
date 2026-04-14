# scan-job — AI 可读项目文档（索引）

> ⚠️ **强制规定：每次功能修改后必须同步更新本文件。**

> **融资发币未上所代币扫描系统**：从 RootData（DrissionPage CDP 登录 + DOM 表格提取）采集已融资项目，筛选已发币但未上任何中心化交易所的代币，通过 CMC API（symbol→id→market-pairs）交叉核对上所状态。提供 Tabler + Petite-Vue 驱动的 Web UI（端口 3600）支持扫描管理、项目浏览和配置。

## 技术栈

| 层 | 技术 |
|---|---|
| 后端 | Python 3.12+ + FastAPI + Uvicorn |
| 前端 | Tabler UI Kit (CDN) + Petite-Vue 0.4 (CDN) |
| 数据库 | SQLite（`data/scan.db`） |
| 爬虫 | DrissionPage CDP（RootData 登录态 DOM 提取） |
| 代理 | `config.get_proxy()` 统一入口（全局单代理） |
| 资产核对 | CMC Pro API（/v1/cryptocurrency/map + /v2/market-pairs） |

## 目录结构

```
scan-job/
├── AI_CONTEXT.md              # 本文件：索引
├── config.py                  # 全局配置（API Key, 代理, get_proxy()）
├── .env                       # 环境变量（CMC_API_KEY, PROXY_URL, ROOTDATA_EMAIL/PASSWORD）
├── .env.example               # 环境变量模板
├── requirements.txt           # Python 依赖
├── core/                      # 核心业务逻辑
│   ├── rootdata_scraper.py    # RootData CDP 爬虫（DrissionPage + 登录 + DOM 表格提取）
│   ├── cryptorank_scraper.py  # CryptoRank 融资爬虫（已停用 — VIP 限制）
│   ├── cmc_verifier.py        # CMC API 资产核对（symbol→id→market-pairs）
│   ├── scanner.py             # 主扫描调度引擎（RootData → CMC 核对 → 入库）
│   └── db.py                  # SQLite 数据库管理（3 表）
├── data/
│   └── scan.db                # SQLite 数据库（运行时生成）
└── web/
    ├── server.py              # FastAPI 应用入口（端口 3600）
    └── frontend/              # 前端静态文件
        ├── index.html         # 入口（重定向到 /dashboard）
        ├── dashboard.html     # 仪表盘（统计卡片 + 未上所 Top 20）
        ├── projects.html      # 项目列表（搜索/筛选/分页/CSV 导出）
        ├── scan.html          # 扫描管理（启动/停止/实时日志/历史）
        ├── settings.html      # 设置（CMC API Key + 代理配置）
        └── static/
            ├── app.js         # 公共 JS（API 客户端 + 格式化 + Toast）
            └── style.css      # Tabler 深色主题覆盖 + 自定义组件
```

## 数据流

```
┌──────────────────────────────────────────────────┐
│  ① RootData CDP 采集                              │
│     DrissionPage 连接桌面 Chrome                  │
│     登录 cn.rootdata.com → 翻页 DOM 表格提取     │
│     每页 30 项目 → 项目名/轮次/金额/日期/投资方  │
└────────────────────┬─────────────────────────────┘
                     │ 去重入库（project_name 主键）
                     ▼
┌──────────────────────────────────────────────────┐
│  ② CMC API 核对                                    │
│     /v1/cryptocurrency/map → symbol→id            │
│     /v2/cryptocurrency/market-pairs/latest → 交易对│
│     market_pairs = 0 → 未上所                     │
└────────────────────┬─────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────┐
│  SQLite (data/scan.db)                            │
│  projects 表 ← 项目基础信息 + 融资数据            │
│  tokens 表   ← 代币合约 + 上所状态 + CMC 核对     │
│  scan_logs 表 ← 扫描批次日志                      │
└────────────────────┬─────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────┐
│  Web UI (http://localhost:3600)                    │
│  仪表盘 / 项目列表 / 扫描管理 / 设置              │
└──────────────────────────────────────────────────┘
```

## RootData 爬虫架构

```
DrissionPage ChromiumPage(连接桌面 Chrome)
  │
  ├─ 登录检测 → 未登录: 自动填写 email/password → 点击登录
  │                      登录态由桌面 Chrome 保持
  │
  ├─ 导航 /Fundraising → DOM 提取 table.b-table tbody tr
  │   td[0]: 项目名 (div.name 最后一个 span) + logo (img) + 描述 + href
  │   td[1]: 轮次
  │   td[2]: 金额 (万/亿美元 → 归一化为数字)
  │   td[4]: 日期 (MM-DD → YYYY-MM-DD)
  │   td[6]: 投资方 (子 <a> 链接逐个提取)
  │
  └─ 翻页: ElementUI 分页器 li.number 点击
```

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/health` | 健康检查 |
| GET | `/api/stats` | 统计概览 |
| GET | `/api/settings` | 获取配置 |
| POST | `/api/settings` | 更新配置 |
| POST | `/api/scan/start` | 启动全量扫描 |
| GET | `/api/scan/status/{id}` | 查询进度 |
| POST | `/api/scan/stop/{id}` | 停止扫描 |
| GET | `/api/projects` | 项目列表（分页+筛选） |
| GET | `/api/projects/{id}` | 项目详情 |
| GET | `/api/tokens` | 代币列表 |
| POST | `/api/verify` | 手动 CMC 核对 |
| GET | `/api/scan-logs` | 扫描历史 |

## 环境变量

```env
CMC_API_KEY=xxx              # CMC Pro API Key
PROXY_URL=socks5://host:port # 代理地址
PROXY_ENABLED=true           # 代理开关
ROOTDATA_EMAIL=xxx           # RootData 登录邮箱
ROOTDATA_PASSWORD=xxx        # RootData 登录密码
```

## 启动方式

```bash
pip install -r requirements.txt
python web/server.py  # 后端+前端统一端口 3600
```
