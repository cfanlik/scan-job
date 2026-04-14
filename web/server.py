"""
scan-job Web API Server
FastAPI 后端 — 融资代币扫描平台

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

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse

from config import CMC_API_KEY, get_proxy

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
    progress = task.setdefault("progress", [])
    progress.append(msg)
    if len(progress) > MAX_PROGRESS_LINES:
        task["progress"] = [
            f"... (已省略 {len(progress) - MAX_PROGRESS_LINES} 条) ..."
        ] + progress[-MAX_PROGRESS_LINES:]


# ---------- Lifespan ---------- #

@asynccontextmanager
async def _lifespan(app: FastAPI):
    from core.db import init_db
    init_db()
    logger.info("[STARTUP] 数据库已初始化")
    yield


# ---------- App ---------- #
app = FastAPI(
    title="scan-job API",
    description="融资代币扫描平台",
    version="1.0.0",
    lifespan=_lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)


# ---------- Models ---------- #

class ScanRequest(BaseModel):
    max_rootdata_pages: int = 10
    enable_cmc_verify: bool = True


class SettingsUpdate(BaseModel):
    cmc_api_key: str = ""
    proxy_url: str = ""
    proxy_enabled: bool = True


# ---------- Health / Stats ---------- #

@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


@app.get("/api/stats")
async def stats():
    from core.db import get_connection, get_stats
    conn = get_connection()
    try:
        return get_stats(conn)
    finally:
        conn.close()


# ---------- Settings ---------- #

def _mask_key(key: str) -> str:
    if not key or len(key) < 8:
        return "" if not key else "***"
    return f"{key[:4]}***{key[-4:]}"


@app.get("/api/settings")
async def get_settings():
    proxy_url = os.environ.get("PROXY_URL", "")
    proxy_enabled = os.environ.get("PROXY_ENABLED", "true").lower() in ("1", "true", "yes")
    return {
        "cmc_api_key": _mask_key(os.environ.get("CMC_API_KEY", "") or CMC_API_KEY),
        "cmc_configured": bool(os.environ.get("CMC_API_KEY", "") or CMC_API_KEY),
        "proxy_url": proxy_url,
        "proxy_enabled": proxy_enabled,
    }


@app.post("/api/settings")
async def update_settings(s: SettingsUpdate):
    env_path = os.path.join(_PROJECT_ROOT, ".env")
    existing = {}
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    existing[k.strip()] = v.strip()

    if s.cmc_api_key:
        existing["CMC_API_KEY"] = s.cmc_api_key
        os.environ["CMC_API_KEY"] = s.cmc_api_key
    if s.proxy_url:
        existing["PROXY_URL"] = s.proxy_url
        os.environ["PROXY_URL"] = s.proxy_url
    existing["PROXY_ENABLED"] = "true" if s.proxy_enabled else "false"
    os.environ["PROXY_ENABLED"] = existing["PROXY_ENABLED"]

    import config as _cfg
    _cfg.PROXY_ENABLED = s.proxy_enabled
    _cfg.PROXY_URL = os.environ.get("PROXY_URL", "") or _cfg.PROXY_URL

    with open(env_path, "w", encoding="utf-8") as f:
        f.write("# scan-job 配置\n")
        for k, v in existing.items():
            f.write(f"{k}={v}\n")

    return {"ok": True, "message": "配置已保存"}


# ---------- 扫描任务 ---------- #

@app.post("/api/scan/start")
async def scan_start(req: ScanRequest):
    _cleanup_tasks()

    task_id = str(uuid.uuid4())[:8]
    _tasks[task_id] = {
        "task_id": task_id,
        "status": "running",
        "progress": [],
        "result": None,
        "created_at": datetime.now().isoformat(),
    }

    def _run():
        task = _tasks[task_id]
        try:
            from core.scanner import Scanner

            def on_log(msg):
                _append_progress(task, msg)

            proxy = get_proxy()
            cmc_key = os.environ.get("CMC_API_KEY", "") or CMC_API_KEY

            scanner = Scanner(proxy=proxy, cmc_api_key=cmc_key)
            result = scanner.run_full_scan(
                on_log=on_log,
                max_rootdata_pages=req.max_rootdata_pages,
                enable_cmc_verify=req.enable_cmc_verify,
            )

            task["result"] = result
            task["status"] = result.get("status", "done")

        except Exception as e:
            logger.exception("[SCAN] 任务失败")
            task["status"] = "error"
            task["result"] = {"error": str(e)}
            _append_progress(task, f"❌ 失败: {e}")
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


@app.post("/api/scan/stop/{task_id}")
async def scan_stop(task_id: str):
    task = _tasks.get(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    task["status"] = "cancelled"
    task["finished_at"] = datetime.now().isoformat()
    return {"ok": True}


# ---------- 项目列表 ---------- #

@app.get("/api/projects")
async def projects_list(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    source: str = Query(None),
    search: str = Query(None),
):
    from core.db import get_connection, get_projects
    conn = get_connection()
    try:
        rows, total = get_projects(conn, offset, limit, source, search)
        return {"data": rows, "total": total, "offset": offset, "limit": limit}
    finally:
        conn.close()


@app.get("/api/projects/{project_id}")
async def project_detail(project_id: int):
    from core.db import get_connection
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
        if not row:
            raise HTTPException(404, "项目不存在")

        tokens = conn.execute(
            "SELECT * FROM tokens WHERE project_id = ?", (project_id,)
        ).fetchall()

        return {
            "project": dict(row),
            "tokens": [dict(t) for t in tokens],
        }
    finally:
        conn.close()


@app.delete("/api/projects/{project_id}")
async def delete_project(project_id: int):
    from core.db import get_connection
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM projects WHERE id = ?", (project_id,)).fetchone()
        if not row:
            raise HTTPException(404, "项目不存在")
        conn.execute("DELETE FROM tokens WHERE project_id = ?", (project_id,))
        conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
        conn.commit()
        return {"ok": True, "message": f"项目 {project_id} 及关联代币已删除"}
    finally:
        conn.close()


@app.delete("/api/tokens/{token_id}")
async def delete_token(token_id: int):
    from core.db import get_connection
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM tokens WHERE id = ?", (token_id,)).fetchone()
        if not row:
            raise HTTPException(404, "代币不存在")
        conn.execute("DELETE FROM tokens WHERE id = ?", (token_id,))
        conn.commit()
        return {"ok": True, "message": f"代币 {token_id} 已删除"}
    finally:
        conn.close()


@app.delete("/api/data/clear")
async def clear_all_data():
    from core.db import get_connection
    conn = get_connection()
    try:
        t_count = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
        p_count = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
        conn.execute("DELETE FROM tokens")
        conn.execute("DELETE FROM projects")
        conn.execute("DELETE FROM scan_logs")
        conn.commit()
        return {"ok": True, "message": f"已清空 {p_count} 个项目, {t_count} 个代币"}
    finally:
        conn.close()


# ---------- 代币列表 ---------- #

@app.get("/api/tokens")
async def tokens_list(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    not_listed_only: bool = Query(False),
):
    from core.db import get_connection
    conn = get_connection()
    try:
        where = "WHERE t.token_symbol != ''"
        if not_listed_only:
            where += " AND t.cmc_listed = 0 AND t.cr_traded = 0"

        total = conn.execute(
            f"SELECT COUNT(*) FROM tokens t {where}"
        ).fetchone()[0]

        rows = conn.execute(f"""
            SELECT t.*, p.project_name, p.logo, p.total_funding,
                   p.latest_round, p.source, p.investors
            FROM tokens t
            LEFT JOIN projects p ON p.id = t.project_id
            {where}
            ORDER BY t.created_at DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()

        return {"data": [dict(r) for r in rows], "total": total}
    finally:
        conn.close()


# ---------- 手动 CMC 核对 ---------- #

@app.post("/api/verify")
async def manual_verify(symbols: list[str] = None):
    if not symbols:
        raise HTTPException(400, "请提供 symbol 列表")

    cmc_key = os.environ.get("CMC_API_KEY", "") or CMC_API_KEY
    if not cmc_key:
        raise HTTPException(400, "CMC API Key 未配置")

    from core.cmc_verifier import CMCVerifier
    verifier = CMCVerifier(api_key=cmc_key, proxy=get_proxy())
    results = verifier.verify_batch(symbols)
    return {"results": results}


# ---------- 扫描历史 ---------- #

@app.get("/api/scan-logs")
async def scan_logs(limit: int = Query(20, ge=1, le=100)):
    from core.db import get_connection, get_scan_logs
    conn = get_connection()
    try:
        return {"data": get_scan_logs(conn, limit)}
    finally:
        conn.close()


@app.delete("/api/scan-logs/{scan_id}")
async def delete_scan_log(scan_id: str):
    from core.db import get_connection
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM scan_logs WHERE scan_id = ?", (scan_id,)).fetchone()
        if not row:
            raise HTTPException(404, "记录不存在")
        conn.execute("DELETE FROM scan_logs WHERE scan_id = ?", (scan_id,))
        conn.commit()
        return {"ok": True, "message": f"扫描记录 {scan_id} 已删除"}
    finally:
        conn.close()


@app.delete("/api/scan-logs")
async def clear_scan_logs():
    from core.db import get_connection
    conn = get_connection()
    try:
        count = conn.execute("SELECT COUNT(*) FROM scan_logs").fetchone()[0]
        conn.execute("DELETE FROM scan_logs")
        conn.commit()
        return {"ok": True, "message": f"已清空 {count} 条扫描记录"}
    finally:
        conn.close()


# ---------- 静态文件 & 路由 ---------- #

_FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")


@app.get("/", response_class=HTMLResponse)
async def index():
    return RedirectResponse("/dashboard")


# 页面路由 → HTML
for _page in ("dashboard", "projects", "scan", "settings"):
    _html_path = os.path.join(_FRONTEND_DIR, f"{_page}.html")

    def _make_handler(path):
        async def handler():
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return HTMLResponse(f.read())
            raise HTTPException(404, "页面不存在")
        return handler

    app.add_api_route(f"/{_page}", _make_handler(_html_path), methods=["GET"],
                      response_class=HTMLResponse)

# 静态资源
_STATIC_DIR = os.path.join(_FRONTEND_DIR, "static")
if os.path.isdir(_STATIC_DIR):
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=3600, reload=True)
