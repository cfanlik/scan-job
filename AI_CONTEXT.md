# scan-job — AI 可读项目文档（索引）

> ⚠️ **强制规定：每次功能修改后必须同步更新本文件。**

> **融资代币扫描与 Upbit 资方拟合度分析平台**：彻底弃用脆弱的 RootData 网页爬虫，全面转向基于 **CryptoRank 官方 API v3** 进行融资与早期发币项目采集。系统深度借鉴 `select-coin` 生产环境的高可用 **GMGN API 轮询与限流机制**（多 Key 轮询管理、令牌桶严格节流、指数退避重试、7 天本地元数据快照缓存）。结合 **Upbit 官方开放 API** 动态同步在市代币，构建 Upbit 偏好资方图谱（Hashed、Dunamu、DWF、Animoca、Top VC 加权体系），为全网代币输出 0~100 分的 **Upbit 资方拟合度 (Fit Score)**，并实现**融资发币未上交易所 (UNLISTED_GEM)** 与 **Upbit 上币潜力候选 (UPBIT_CANDIDATE)** 的双核心筛选闭环。

## 技术栈

| 层 | 技术 | 说明 |
|---|---|---|
| **后端** | Python 3.12+ + FastAPI + Uvicorn | 统一监听端口 3600，高并发异步任务与状态流转 |
| **前端** | Tabler UI Kit (深色主题) + Petite-Vue 0.4 (CDN) | 现代化响应式面板，提供仪表盘、Upbit 候选榜、项目管理与设置 |
| **数据库** | SQLite（`data/scan.db`） | 5 张表（projects / tokens / scan_logs / upbit_markets / upbit_fund_profiles） |
| **数据采集** | CryptoRank 官方 API v3 客户端 | 纯 REST API 架构，集成 GMGN 调度器、令牌桶节流（10 req/min）与 429 智能退避 |
| **画像分析** | Upbit Profiler 引擎 | Upbit 现货市场实时抓取 + 资方基因库加权拟合度算法 |
| **资产核对** | CMC Pro API（/v1/map + /v2/market-pairs） | 市场交易对数量与大所上市状态交叉核验 |
| **代理支持** | `config.get_proxy()` 统一入口 | SOCKS5 代理无缝穿越支持 |

## 目录结构

```
scan-job/
├── AI_CONTEXT.md              # 架构文档与索引
├── config.py                  # 全局配置（CryptoRank 多 Key 轮询分配器, CMC API Key, 代理）
├── .env                       # 环境变量
├── .env.example               # 环境变量模板
├── requirements.txt           # Python 依赖（纯轻量，移除无头浏览器）
├── core/                      # 核心业务逻辑
│   ├── cryptorank_client.py   # CryptoRank API v3 客户端（GMGN 调度机制 + 令牌桶 + 本地快照）
│   ├── upbit_profiler.py      # Upbit 市场同步与资方拟合度打分算法
│   ├── cmc_verifier.py        # CMC API 资产核对
│   ├── scanner.py             # 主扫描调度引擎（CryptoRank → Upbit Profiler → CMC 核对 → 入库）
│   └── db.py                  # SQLite 数据库管理（5 表）
├── data/
│   ├── scan.db                # SQLite 数据库（运行时生成）
│   └── cryptorank_cache.db    # CryptoRank API 本地元数据缓存库
└── web/
    ├── server.py              # FastAPI 应用入口（端口 3600）
    └── frontend/              # 前端静态文件
        ├── index.html         # 入口重定向
        ├── dashboard.html     # 仪表盘（Upbit 候选标的 + 未上所 Gems 看板）
        ├── projects.html      # 项目列表（Upbit 拟合度 / 命中资方 / 筛选 / 导出）
        ├── scan.html          # 扫描管理（启动 / 实时日志 / 历史）
        ├── settings.html      # 系统设置（CryptoRank Key / CMC Key / 代理）
        └── static/
            ├── app.js         # API 客户端 + 格式化工具
            └── style.css      # Tabler 主题样式
```

## 数据流与算法闭环

```
┌────────────────────────────────────────────────────────┐
│  ① CryptoRank API v3 采集                              │
│     currencies/map (3.9万代币) + drophunting/map       │
│     多 Key 轮换 + 令牌桶 10 req/min + 本地快照缓存     │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  ② Upbit 市场同步与资方画像偏好计算                    │
│     api.upbit.com/v1/market/all (358 个在线 Symbol)    │
│     Hashed / Dunamu / DWF / Animoca / Top VC 特征匹配  │
│     计算 0~100 分 Upbit Fit Score (S/A/B 分级)         │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  ③ CMC API 交叉核验与双核心目标归类                    │
│     CMC market-pairs 验证交易对状态                    │
│     目标 A: 融资发币未上大所 (UNLISTED_GEM)            │
│     目标 B: Upbit 潜在上币候选 (UPBIT_CANDIDATE)       │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  ④ SQLite (data/scan.db) 原子物化写穿                  │
│     Web UI (http://localhost:3600) 实时图表与看板呈现   │
└────────────────────────────────────────────────────────┘
```

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/health` | 健康检查 |
| GET | `/api/stats` | 统计概览（含 Upbit 候选数与未上所数） |
| GET | `/api/tokens/upbit-candidates` | Upbit 潜在上币候选列表（按拟合度排序） |
| GET | `/api/tokens/unlisted-gems` | 融资发币未上交易所代币列表 |
| GET | `/api/upbit/profile` | Upbit 资方偏好特征基因库 |
| GET | `/api/projects` | 项目列表（支持 unlisted_only / upbit_candidate_only） |
| POST | `/api/scan/start` | 启动扫描调度任务 |
| GET | `/api/scan/status/{id}` | 查询任务进度与日志 |
| GET | `/api/settings` | 系统设置查询 |
| POST | `/api/settings` | 系统设置修改 |
