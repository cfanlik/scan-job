"""
scan-job Web API Server
FastAPI 后端 — CryptoRank API v3 + Upbit 资方拟合度 + 融资代币扫描平台

启动: python web/server.py
端口: 3600
"""
import os
import sys
import uuid
import json
import time
import threading
import logging
import gc
from datetime import datetime
from contextlib import asynccontextmanager

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.makedirs(os.path.join(_PROJECT_ROOT, "logs"), exist_ok=True)

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse

from config import CMC_API_KEY, get_proxy, CRYPTORANK_API_KEYS, acquire_cryptorank_key

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("web-api")

# ---------- 内存管理 ---------- #
MAX_TASK_KEEP = 5
MAX_TASK_AGE_H = 2
MAX_PROGRESS_LINES = 200

_tasks: dict = {}


def _cleanup_tasks():
    now = datetime.now()
    finished_ids = [
        tid for tid, t in _tasks.items()
        if t.get("status") in ("done", "error", "cancelled")
    ]
    removed = 0
    for tid in list(finished_ids):
        t = _tasks.get(tid)
        if not t:
            continue
        fa = t.get("finished_at") or t.get("created_at", "")
        try:
            ft = datetime.fromisoformat(fa)
            if (now - ft).total_seconds() / 3600 > MAX_TASK_AGE_H:
                del _tasks[tid]
                finished_ids.remove(tid)
                removed += 1
        except Exception:
            pass

    if len(finished_ids) > MAX_TASK_KEEP:
        finished_ids.sort(key=lambda x: _tasks.get(x, {}).get("created_at", ""))
        for tid in finished_ids[:len(finished_ids) - MAX_TASK_KEEP]:
            if tid in _tasks:
                del _tasks[tid]
                removed += 1

    if removed:
        gc.collect()
        logger.info("[CLEANUP] 清理 %d 个旧任务", removed)


def _append_progress(task: dict, msg: str):
    task["progress"].append(msg)
    if len(task["progress"]) > MAX_PROGRESS_LINES:
        task["progress"] = task["progress"][-MAX_PROGRESS_LINES:]


# ---------- 应用生命周期 ---------- #

@asynccontextmanager
async def _lifespan(app: FastAPI):
    from core.db import init_db
    init_db()
    logger.info("[STARTUP] 数据库已初始化")
    yield
    _tasks.clear()
    gc.collect()
    logger.info("[SHUTDOWN] 服务已安全关闭")


app = FastAPI(
    title="scan-job API",
    description="CryptoRank API v3 采集引擎 + Upbit 资方拟合度 + 融资代币扫描",
    version="2.0.0",
    lifespan=_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")
_STATIC_DIR = os.path.join(_FRONTEND_DIR, "static")
if os.path.isdir(_STATIC_DIR):
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


# ---------- 状态与概览 ---------- #

@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "scan-job", "version": "2.0.0"}


@app.get("/api/stats")
async def stats():
    from core.db import get_connection, get_stats
    conn = get_connection()
    try:
        return get_stats(conn)
    finally:
        conn.close()


def _mask_key(key: str) -> str:
    if not key or len(key) < 8:
        return "****"
    return key[:4] + "****" + key[-4:]


@app.get("/api/settings")
async def get_settings():
    from config import PROXY_URL, PROXY_ENABLED, CRYPTORANK_API_KEYS
    key = acquire_cryptorank_key()
    return {
        "cmc_api_key_set": bool(CMC_API_KEY),
        "cmc_api_key_masked": _mask_key(CMC_API_KEY),
        "cryptorank_keys_count": len(CRYPTORANK_API_KEYS),
        "cryptorank_key_masked": _mask_key(key or ""),
        "proxy_url": PROXY_URL,
        "proxy_enabled": PROXY_ENABLED,
    }


class SettingsUpdate(BaseModel):
    cmc_api_key: str | None = None
    cryptorank_api_key: str | None = None
    proxy_url: str | None = None
    proxy_enabled: bool | None = None


@app.post("/api/settings")
async def update_settings(s: SettingsUpdate):
    env_path = os.path.join(_PROJECT_ROOT, ".env")
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

    updates = {}
    if s.cmc_api_key is not None:
        updates["CMC_API_KEY"] = s.cmc_api_key
    if s.cryptorank_api_key is not None:
        updates["CRYPTORANK_API_KEY"] = s.cryptorank_api_key
    if s.proxy_url is not None:
        updates["PROXY_URL"] = s.proxy_url
    if s.proxy_enabled is not None:
        updates["PROXY_ENABLED"] = "true" if s.proxy_enabled else "false"

    new_lines = []
    seen = set()
    for line in lines:
        k = line.split("=")[0].strip() if "=" in line else ""
        if k in updates:
            new_lines.append(f"{k}={updates[k]}\n")
            seen.add(k)
        else:
            new_lines.append(line)
    for k, v in updates.items():
        if k not in seen:
            new_lines.append(f"{k}={v}\n")

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

    from dotenv import load_dotenv
    load_dotenv(env_path, override=True)

    return {"ok": True, "message": "配置已保存并生效"}


# ---------- 扫描管理 ---------- #

class ScanRequest(BaseModel):
    mode: str = "auto"
    max_pages: int = 0
    enable_cmc_verify: bool = True


@app.post("/api/scan/start")
async def scan_start(req: ScanRequest):
    _cleanup_tasks()
    for tid, t in _tasks.items():
        if t.get("status") == "running":
            raise HTTPException(409, f"扫描任务 {tid} 正在运行中")

    task_id = uuid.uuid4().hex[:8]
    _tasks[task_id] = {
        "status": "running",
        "progress": [],
        "created_at": datetime.now().isoformat(),
        "finished_at": None,
        "result": None,
        "mode": req.mode,
    }

    def _run():
        task = _tasks[task_id]
        log_file = os.path.join(_PROJECT_ROOT, "logs", f"task_{task_id}.log")

        def on_log(msg):
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            line = f"[{ts}] {msg}"
            logger.info("[Task %s] %s", task_id, msg)
            _append_progress(task, line)
            try:
                with open(log_file, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception:
                pass

        try:
            from core.scanner import Scanner
            from config import CMC_API_KEY, get_proxy

            scanner = Scanner(
                proxy=get_proxy(),
                cmc_api_key=CMC_API_KEY,
            )
            result = scanner.run_scan(mode=req.mode, on_log=on_log)
            task["result"] = result
            task["status"] = result.get("status", "done")
        except Exception as e:
            logger.exception("[Task %s] 执行失败", task_id)
            task["status"] = "error"
            task["result"] = {"error": str(e)}
            on_log(f"任务异常失败: {e}")
        finally:
            task["finished_at"] = datetime.now().isoformat()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {"task_id": task_id, "status": "running"}


@app.get("/api/scan/status/{task_id}")
async def scan_status(task_id: str):
    task = _tasks.get(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    return task


@app.get("/api/scan/active")
async def get_active_task():
    for tid, t in _tasks.items():
        if t.get("status") == "running":
            return {"task_id": tid, "status": "running", "created_at": t.get("created_at")}
    return {"task_id": None, "status": "idle"}


# ---------- Upbit 拟合度与未上所专用 API ---------- #

@app.get("/api/tokens/upbit-candidates")
async def upbit_candidates(min_score: int = Query(70, ge=0, le=100), limit: int = Query(50, ge=1, le=200)):
    """获取 Upbit 潜在上币候选（高拟合度未上 Upbit 代币）。"""
    from core.db import get_connection, get_upbit_candidates
    conn = get_connection()
    try:
        data = get_upbit_candidates(conn, min_score=min_score, limit=limit)
        return {"data": data, "count": len(data), "min_score": min_score}
    finally:
        conn.close()


@app.get("/api/tokens/unlisted-gems")
async def unlisted_gems(limit: int = Query(50, ge=1, le=200)):
    """获取融资发币未上任何中心化大所的代币。"""
    from core.db import get_connection, get_unlisted_gems
    conn = get_connection()
    try:
        data = get_unlisted_gems(conn, limit=limit)
        return {"data": data, "count": len(data)}
    finally:
        conn.close()


@app.get("/api/upbit/profile")
async def upbit_profile():
    """获取 Upbit 资方偏好特征基因图谱。"""
    from core.upbit_profiler import UPBIT_BASELINE_FUNDS, UpbitProfiler
    from config import get_proxy
    profiler = UpbitProfiler(proxy=get_proxy())
    _, symbols = profiler.fetch_upbit_markets()

    funds_list = []
    for k, v in UPBIT_BASELINE_FUNDS.items():
        funds_list.append({
            "key": k,
            "name": v["name"],
            "tier": v["tier"],
            "base_weight": v["base_weight"],
            "upbit_correlation": v["upbit_corr"],
        })
    funds_list.sort(key=lambda x: (x["tier"], -x["base_weight"]))

    return {
        "total_upbit_symbols": len(symbols),
        "total_baseline_funds": len(funds_list),
        "funds": funds_list,
    }


# ---------- 项目与代币查询 ---------- #

@app.get("/api/projects")
async def projects_list(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    source: str | None = None,
    search: str | None = None,
    unlisted_only: bool = False,
    upbit_candidate_only: bool = False,
):
    from core.db import get_connection, get_projects
    conn = get_connection()
    try:
        offset = (page - 1) * limit
        rows, total = get_projects(
            conn, offset=offset, limit=limit, source=source, search=search,
            unlisted_only=unlisted_only, upbit_candidate_only=upbit_candidate_only
        )
        return {
            "data": rows,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": max(1, (total + limit - 1) // limit),
        }
    finally:
        conn.close()


# ---------- 扫描历史 ---------- #

@app.get("/api/scan-logs")
async def scan_logs(limit: int = Query(20, ge=1, le=100)):
    from core.db import get_connection, get_scan_logs
    conn = get_connection()
    try:
        return {"data": get_scan_logs(conn, limit)}
    finally:
        conn.close()


# ---------- 页面路由 ---------- #

@app.get("/", response_class=HTMLResponse)
async def index():
    return RedirectResponse("/dashboard")


for _page in ("dashboard", "projects", "scan", "settings"):
    _html_path = os.path.join(_FRONTEND_DIR, f"{_page}.html")

    def _make_handler(path):
        async def handler():
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return HTMLResponse(f.read())
            raise HTTPException(404, "页面不存在")
        return handler

    app.add_api_route(f"/{_page}", _make_handler(_html_path), methods=["GET"], response_class=HTMLResponse)


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=3600, reload=False)
