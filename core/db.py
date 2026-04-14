"""
SQLite 数据库管理 — data/scan.db
表: projects / tokens / scan_logs
"""
import os
import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger("scan-db")

_DB_DIR  = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
_DB_PATH = os.path.join(_DB_DIR, "scan.db")

# 基础建表 Schema（不含新增迁移列，保证对旧库兼容）
_BASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_name TEXT NOT NULL,
    logo TEXT,
    description TEXT,
    tags TEXT,
    source TEXT,
    rootdata_id INTEGER,
    cryptorank_slug TEXT,
    total_funding REAL,
    latest_round TEXT,
    latest_round_date TEXT,
    investors TEXT,
    website TEXT,
    twitter TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(project_name)
);
CREATE TABLE IF NOT EXISTS tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER REFERENCES projects(id),
    token_symbol TEXT,
    token_name TEXT,
    contract_address TEXT,
    chain TEXT,
    exchanges TEXT,
    cmc_listed INTEGER DEFAULT 0,
    cmc_market_pairs INTEGER DEFAULT 0,
    cr_traded INTEGER DEFAULT 0,
    price REAL,
    market_cap REAL,
    fully_diluted_mcap REAL,
    last_verified_at TEXT,
    verification_source TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(token_symbol, chain)
);
CREATE TABLE IF NOT EXISTS scan_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id TEXT NOT NULL,
    scan_type TEXT,
    status TEXT DEFAULT 'running',
    total_projects INTEGER DEFAULT 0,
    funded_with_token INTEGER DEFAULT 0,
    not_listed INTEGER DEFAULT 0,
    cmc_verified INTEGER DEFAULT 0,
    started_at TEXT DEFAULT (datetime('now')),
    finished_at TEXT,
    error_message TEXT
);
"""

# 增量迁移列（对旧库执行 ALTER TABLE，新库由 _BASE_SCHEMA 后补）
_MIGRATIONS = [
    "ALTER TABLE projects ADD COLUMN rootdata_url TEXT",
    "CREATE INDEX IF NOT EXISTS idx_projects_rootdata_url ON projects(rootdata_url)",
    "ALTER TABLE scan_logs ADD COLUMN new_projects INTEGER DEFAULT 0",
]


def get_connection() -> sqlite3.Connection:
    os.makedirs(_DB_DIR, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_connection()
    # 1. 建基础表（幂等）
    conn.executescript(_BASE_SCHEMA)
    conn.commit()
    # 2. 迁移新列（忽略"duplicate column"错误）
    for sql in _MIGRATIONS:
        try:
            conn.execute(sql)
            conn.commit()
        except Exception:
            pass
    conn.close()
    logger.info("[DB] 数据库初始化完成: %s", _DB_PATH)


def upsert_project(conn: sqlite3.Connection, data: dict) -> int:
    """插入或更新项目，返回 project_id。"""
    now = datetime.now().isoformat()
    row = conn.execute(
        "SELECT id FROM projects WHERE project_name = ?",
        (data["project_name"],),
    ).fetchone()

    if row:
        pid = row[0]
        conn.execute("""
            UPDATE projects SET
                logo = COALESCE(?, logo),
                description = COALESCE(?, description),
                tags = COALESCE(?, tags),
                source = CASE
                    WHEN source != ? AND source NOT LIKE '%both%' THEN 'both'
                    ELSE COALESCE(?, source)
                END,
                rootdata_id = COALESCE(?, rootdata_id),
                rootdata_url = COALESCE(?, rootdata_url),
                cryptorank_slug = COALESCE(?, cryptorank_slug),
                total_funding = COALESCE(?, total_funding),
                latest_round = COALESCE(?, latest_round),
                latest_round_date = COALESCE(?, latest_round_date),
                investors = COALESCE(?, investors),
                website = COALESCE(?, website),
                twitter = COALESCE(?, twitter),
                updated_at = ?
            WHERE id = ?
        """, (
            data.get("logo"), data.get("description"), data.get("tags"),
            data.get("source", ""), data.get("source"),
            data.get("rootdata_id"), data.get("rootdata_url"),
            data.get("cryptorank_slug"),
            data.get("total_funding"), data.get("latest_round"),
            data.get("latest_round_date"), data.get("investors"),
            data.get("website"), data.get("twitter"),
            now, pid,
        ))
    else:
        cur = conn.execute("""
            INSERT INTO projects (
                project_name, logo, description, tags, source,
                rootdata_id, rootdata_url, cryptorank_slug, total_funding,
                latest_round, latest_round_date, investors,
                website, twitter, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["project_name"], data.get("logo"), data.get("description"),
            data.get("tags"), data.get("source"),
            data.get("rootdata_id"), data.get("rootdata_url"),
            data.get("cryptorank_slug"),
            data.get("total_funding"), data.get("latest_round"),
            data.get("latest_round_date"), data.get("investors"),
            data.get("website"), data.get("twitter"),
            now, now,
        ))
        pid = cur.lastrowid

    conn.commit()
    return pid


def upsert_token(conn: sqlite3.Connection, project_id: int, data: dict) -> int:
    """插入或更新代币信息。"""
    row = conn.execute(
        "SELECT id FROM tokens WHERE token_symbol = ? AND chain = ?",
        (data.get("token_symbol", ""), data.get("chain", "")),
    ).fetchone()

    if row:
        tid = row[0]
        conn.execute("""
            UPDATE tokens SET
                project_id = ?,
                token_name = COALESCE(?, token_name),
                contract_address = COALESCE(?, contract_address),
                exchanges = COALESCE(?, exchanges),
                cmc_listed = COALESCE(?, cmc_listed),
                cmc_market_pairs = COALESCE(?, cmc_market_pairs),
                cr_traded = COALESCE(?, cr_traded),
                price = COALESCE(?, price),
                market_cap = COALESCE(?, market_cap),
                fully_diluted_mcap = COALESCE(?, fully_diluted_mcap),
                last_verified_at = COALESCE(?, last_verified_at),
                verification_source = COALESCE(?, verification_source)
            WHERE id = ?
        """, (
            project_id,
            data.get("token_name"), data.get("contract_address"),
            data.get("exchanges"),
            data.get("cmc_listed"), data.get("cmc_market_pairs"),
            data.get("cr_traded"),
            data.get("price"), data.get("market_cap"),
            data.get("fully_diluted_mcap"),
            data.get("last_verified_at"), data.get("verification_source"),
            tid,
        ))
    else:
        cur = conn.execute("""
            INSERT INTO tokens (
                project_id, token_symbol, token_name, contract_address,
                chain, exchanges, cmc_listed, cmc_market_pairs, cr_traded,
                price, market_cap, fully_diluted_mcap,
                last_verified_at, verification_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            project_id,
            data.get("token_symbol", ""), data.get("token_name"),
            data.get("contract_address"), data.get("chain", ""),
            data.get("exchanges"),
            data.get("cmc_listed", 0), data.get("cmc_market_pairs", 0),
            data.get("cr_traded", 0),
            data.get("price"), data.get("market_cap"),
            data.get("fully_diluted_mcap"),
            data.get("last_verified_at"), data.get("verification_source"),
        ))
        tid = cur.lastrowid

    conn.commit()
    return tid


def get_known_rootdata_urls(conn: sqlite3.Connection) -> set[str]:
    """返回 DB 中已有的 rootdata_url 集合，用于增量去重。"""
    rows = conn.execute(
        "SELECT rootdata_url FROM projects WHERE rootdata_url IS NOT NULL AND rootdata_url != ''"
    ).fetchall()
    return {r[0] for r in rows}


def get_scan_meta(conn: sqlite3.Connection) -> dict:
    """返回扫描元信息：上次全量/增量时间、总项目数、是否可增量。"""
    total = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]

    last_full = conn.execute(
        "SELECT finished_at FROM scan_logs WHERE scan_type='full' AND status='done' "
        "ORDER BY finished_at DESC LIMIT 1"
    ).fetchone()

    last_inc = conn.execute(
        "SELECT finished_at FROM scan_logs WHERE scan_type='incremental' AND status='done' "
        "ORDER BY finished_at DESC LIMIT 1"
    ).fetchone()

    return {
        "total_projects": total,
        "last_full_scan_at": last_full[0] if last_full else None,
        "last_incremental_at": last_inc[0] if last_inc else None,
        "can_incremental": total > 0,
    }


def create_scan_log(conn: sqlite3.Connection, scan_id: str,
                    scan_type: str = "full") -> int:
    cur = conn.execute(
        "INSERT INTO scan_logs (scan_id, scan_type) VALUES (?, ?)",
        (scan_id, scan_type),
    )
    conn.commit()
    return cur.lastrowid


def update_scan_log(conn: sqlite3.Connection, scan_id: str, **kwargs):
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [scan_id]
    conn.execute(f"UPDATE scan_logs SET {sets} WHERE scan_id = ?", vals)
    conn.commit()


def get_projects(conn: sqlite3.Connection, offset=0, limit=50,
                 source=None, search=None) -> tuple[list[dict], int]:
    """查询项目列表，返回 (rows, total)。"""
    where  = []
    params = []
    if source:
        where.append("p.source LIKE ?")
        params.append(f"%{source}%")
    if search:
        where.append("p.project_name LIKE ?")
        params.append(f"%{search}%")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    total = conn.execute(
        f"SELECT COUNT(*) FROM projects p {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(f"""
        SELECT p.*, t.token_symbol, t.contract_address, t.chain,
               t.exchanges, t.cmc_listed, t.cmc_market_pairs, t.cr_traded,
               t.price, t.market_cap, t.fully_diluted_mcap
        FROM projects p
        LEFT JOIN tokens t ON t.project_id = p.id
        {where_sql}
        ORDER BY p.updated_at DESC
        LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()

    return [dict(r) for r in rows], total


def get_stats(conn: sqlite3.Connection) -> dict:
    """统计概览。"""
    total      = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    with_token = conn.execute(
        "SELECT COUNT(DISTINCT project_id) FROM tokens WHERE token_symbol != ''"
    ).fetchone()[0]
    not_listed = conn.execute(
        "SELECT COUNT(DISTINCT project_id) FROM tokens "
        "WHERE cmc_listed = 0 AND cr_traded = 0 AND token_symbol != ''"
    ).fetchone()[0]
    last_scan  = conn.execute(
        "SELECT * FROM scan_logs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()

    return {
        "total_projects": total,
        "with_token":     with_token,
        "not_listed":     not_listed,
        "last_scan":      dict(last_scan) if last_scan else None,
    }


def get_scan_logs(conn: sqlite3.Connection, limit=20) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM scan_logs ORDER BY started_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_projects_without_token(conn: sqlite3.Connection, limit: int = 0) -> list[dict]:
    """查询无关联 token 的项目列表。"""
    sql = """
        SELECT p.id, p.project_name, p.rootdata_url
        FROM projects p
        WHERE p.id NOT IN (
            SELECT project_id FROM tokens WHERE token_symbol != '' AND token_symbol IS NOT NULL
        )
        ORDER BY p.id
    """
    if limit > 0:
        sql += f" LIMIT {limit}"
    rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]


if __name__ == "__main__":
    init_db()
    print(f"数据库已创建: {_DB_PATH}")
