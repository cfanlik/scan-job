"""
SQLite 数据库管理 — data/scan.db (V2 增强版)
表: projects / tokens / scan_logs / upbit_markets / upbit_fund_profiles
"""
import os
import json
import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger("scan-db")

_DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
_DB_PATH = os.path.join(_DB_DIR, "scan.db")

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
    upbit_fit_score INTEGER DEFAULT 0,
    upbit_listed INTEGER DEFAULT 0,
    matched_upbit_backers TEXT DEFAULT '[]',
    unlisted_gem_flag INTEGER DEFAULT 0,
    has_spot INTEGER DEFAULT 0,
    has_futures INTEGER DEFAULT 0,
    spot_exchanges TEXT DEFAULT '[]',
    futures_exchanges TEXT DEFAULT '[]',
    coinbase_listed INTEGER DEFAULT 0,
    sector_name TEXT,
    fit_breakdown TEXT DEFAULT '{}',
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
    upbit_candidates INTEGER DEFAULT 0,
    started_at TEXT DEFAULT (datetime('now')),
    finished_at TEXT,
    error_message TEXT,
    new_projects INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS upbit_markets (
    market TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    korean_name TEXT,
    english_name TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS upbit_fund_profiles (
    fund_name TEXT PRIMARY KEY,
    tier INTEGER DEFAULT 3,
    weight REAL DEFAULT 0.2,
    upbit_corr REAL DEFAULT 0.8,
    sample_projects TEXT DEFAULT '[]',
    updated_at TEXT DEFAULT (datetime('now'))
);
"""

_MIGRATIONS = [
    "ALTER TABLE projects ADD COLUMN rootdata_url TEXT",
    "CREATE INDEX IF NOT EXISTS idx_projects_rootdata_url ON projects(rootdata_url)",
    "ALTER TABLE scan_logs ADD COLUMN new_projects INTEGER DEFAULT 0",
    "ALTER TABLE scan_logs ADD COLUMN upbit_candidates INTEGER DEFAULT 0",
    "ALTER TABLE tokens ADD COLUMN upbit_fit_score INTEGER DEFAULT 0",
    "ALTER TABLE tokens ADD COLUMN upbit_listed INTEGER DEFAULT 0",
    "ALTER TABLE tokens ADD COLUMN matched_upbit_backers TEXT DEFAULT '[]'",
    "ALTER TABLE tokens ADD COLUMN unlisted_gem_flag INTEGER DEFAULT 0",
    "ALTER TABLE tokens ADD COLUMN has_spot INTEGER DEFAULT 0",
    "ALTER TABLE tokens ADD COLUMN has_futures INTEGER DEFAULT 0",
    "ALTER TABLE tokens ADD COLUMN spot_exchanges TEXT DEFAULT '[]'",
    "ALTER TABLE tokens ADD COLUMN futures_exchanges TEXT DEFAULT '[]'",
    "ALTER TABLE tokens ADD COLUMN coinbase_listed INTEGER DEFAULT 0",
    "ALTER TABLE tokens ADD COLUMN sector_name TEXT",
    "ALTER TABLE tokens ADD COLUMN fit_breakdown TEXT DEFAULT '{}'",
    "CREATE INDEX IF NOT EXISTS idx_tokens_upbit_score ON tokens(upbit_fit_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_tokens_unlisted_gem ON tokens(unlisted_gem_flag)",
    "CREATE INDEX IF NOT EXISTS idx_tokens_spot_futures ON tokens(has_spot, has_futures)",
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
    conn.executescript(_BASE_SCHEMA)
    conn.commit()
    for sql in _MIGRATIONS:
        try:
            conn.execute(sql)
            conn.commit()
        except Exception:
            pass
    conn.close()
    logger.info("[DB] 数据库初始化完成: %s", _DB_PATH)


def upsert_project(conn: sqlite3.Connection, data: dict) -> int:
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
    row = conn.execute(
        "SELECT id FROM tokens WHERE token_symbol = ? AND chain = ?",
        (data.get("token_symbol", ""), data.get("chain", "")),
    ).fetchone()

    matched_str = data.get("matched_upbit_backers")
    if isinstance(matched_str, (list, dict)):
        matched_str = json.dumps(matched_str, ensure_ascii=False)

    spot_ex_str = data.get("spot_exchanges")
    if isinstance(spot_ex_str, list):
        spot_ex_str = json.dumps(spot_ex_str, ensure_ascii=False)

    fut_ex_str = data.get("futures_exchanges")
    if isinstance(fut_ex_str, list):
        fut_ex_str = json.dumps(fut_ex_str, ensure_ascii=False)

    breakdown_str = data.get("fit_breakdown")
    if isinstance(breakdown_str, dict):
        breakdown_str = json.dumps(breakdown_str, ensure_ascii=False)

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
                verification_source = COALESCE(?, verification_source),
                upbit_fit_score = COALESCE(?, upbit_fit_score),
                upbit_listed = COALESCE(?, upbit_listed),
                matched_upbit_backers = COALESCE(?, matched_upbit_backers),
                unlisted_gem_flag = COALESCE(?, unlisted_gem_flag),
                has_spot = COALESCE(?, has_spot),
                has_futures = COALESCE(?, has_futures),
                spot_exchanges = COALESCE(?, spot_exchanges),
                futures_exchanges = COALESCE(?, futures_exchanges),
                coinbase_listed = COALESCE(?, coinbase_listed),
                sector_name = COALESCE(?, sector_name),
                fit_breakdown = COALESCE(?, fit_breakdown)
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
            data.get("upbit_fit_score"), data.get("upbit_listed"),
            matched_str, data.get("unlisted_gem_flag"),
            data.get("has_spot"), data.get("has_futures"),
            spot_ex_str, fut_ex_str, data.get("coinbase_listed"),
            data.get("sector_name"), breakdown_str,
            tid,
        ))
    else:
        cur = conn.execute("""
            INSERT INTO tokens (
                project_id, token_symbol, token_name, contract_address,
                chain, exchanges, cmc_listed, cmc_market_pairs, cr_traded,
                price, market_cap, fully_diluted_mcap,
                last_verified_at, verification_source,
                upbit_fit_score, upbit_listed, matched_upbit_backers, unlisted_gem_flag,
                has_spot, has_futures, spot_exchanges, futures_exchanges, coinbase_listed,
                sector_name, fit_breakdown
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            data.get("upbit_fit_score", 0), data.get("upbit_listed", 0),
            matched_str or "[]", data.get("unlisted_gem_flag", 0),
            data.get("has_spot", 0), data.get("has_futures", 0),
            spot_ex_str or "[]", fut_ex_str or "[]", data.get("coinbase_listed", 0),
            data.get("sector_name", ""), breakdown_str or "{}"
        ))
        tid = cur.lastrowid

    conn.commit()
    return tid


def upsert_upbit_markets(conn: sqlite3.Connection, markets: list[dict]):
    now = datetime.now().isoformat()
    for m in markets:
        conn.execute("""
            INSERT INTO upbit_markets (market, symbol, korean_name, english_name, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(market) DO UPDATE SET
                symbol = excluded.symbol,
                korean_name = excluded.korean_name,
                english_name = excluded.english_name,
                updated_at = excluded.updated_at
        """, (m["market"], m["symbol"], m.get("korean_name", ""), m.get("english_name", ""), now))
    conn.commit()


def get_upbit_candidates(conn: sqlite3.Connection, min_score: int = 70, limit: int = 50) -> list[dict]:
    rows = conn.execute("""
        SELECT p.project_name, p.logo, p.total_funding, p.latest_round, p.investors,
               t.token_symbol, t.contract_address, t.chain, t.price, t.market_cap,
               t.upbit_fit_score, t.upbit_listed, t.matched_upbit_backers, t.unlisted_gem_flag,
               t.has_spot, t.has_futures, t.spot_exchanges, t.futures_exchanges, t.coinbase_listed,
               t.sector_name, t.fit_breakdown
        FROM tokens t
        JOIN projects p ON p.id = t.project_id
        WHERE t.upbit_listed = 0 AND t.upbit_fit_score >= ?
        ORDER BY t.upbit_fit_score DESC, p.total_funding DESC
        LIMIT ?
    """, (min_score, limit)).fetchall()
    res = []
    for r in rows:
        d = dict(r)
        for k in ("matched_upbit_backers", "spot_exchanges", "futures_exchanges", "fit_breakdown"):
            if isinstance(d.get(k), str):
                try:
                    d[k] = json.loads(d[k])
                except Exception:
                    pass
        res.append(d)
    return res


def get_unlisted_gems(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute("""
        SELECT p.project_name, p.logo, p.total_funding, p.latest_round, p.investors,
               t.token_symbol, t.contract_address, t.chain, t.price, t.market_cap,
               t.cmc_listed, t.cmc_market_pairs, t.upbit_fit_score, t.matched_upbit_backers,
               t.has_spot, t.has_futures, t.spot_exchanges, t.futures_exchanges, t.coinbase_listed,
               t.sector_name, t.fit_breakdown
        FROM tokens t
        JOIN projects p ON p.id = t.project_id
        WHERE t.unlisted_gem_flag = 1
        ORDER BY p.total_funding DESC, t.upbit_fit_score DESC
        LIMIT ?
    """, (limit,)).fetchall()
    res = []
    for r in rows:
        d = dict(r)
        for k in ("matched_upbit_backers", "spot_exchanges", "futures_exchanges", "fit_breakdown"):
            if isinstance(d.get(k), str):
                try:
                    d[k] = json.loads(d[k])
                except Exception:
                    pass
        res.append(d)
    return res


def create_scan_log(conn: sqlite3.Connection, scan_id: str, scan_type: str = "full") -> int:
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
                 source=None, search=None, unlisted_only=False,
                 upbit_candidate_only=False) -> tuple[list[dict], int]:
    where = []
    params = []
    if source:
        where.append("p.source LIKE ?")
        params.append(f"%{source}%")
    if search:
        where.append("(p.project_name LIKE ? OR t.token_symbol LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    if unlisted_only:
        where.append("t.unlisted_gem_flag = 1")
    if upbit_candidate_only:
        where.append("t.upbit_listed = 0 AND t.upbit_fit_score >= 70")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    total = conn.execute(f"""
        SELECT COUNT(*)
        FROM projects p
        LEFT JOIN tokens t ON t.project_id = p.id
        {where_sql}
    """, params).fetchone()[0]

    rows = conn.execute(f"""
        SELECT p.*, t.token_symbol, t.contract_address, t.chain,
               t.exchanges, t.cmc_listed, t.cmc_market_pairs, t.cr_traded,
               t.price, t.market_cap, t.fully_diluted_mcap,
               t.upbit_fit_score, t.upbit_listed, t.matched_upbit_backers, t.unlisted_gem_flag,
               t.has_spot, t.has_futures, t.spot_exchanges, t.futures_exchanges, t.coinbase_listed,
               t.sector_name, t.fit_breakdown
        FROM projects p
        LEFT JOIN tokens t ON t.project_id = p.id
        {where_sql}
        ORDER BY t.upbit_fit_score DESC, p.updated_at DESC
        LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()

    res = []
    for r in rows:
        d = dict(r)
        for k in ("matched_upbit_backers", "spot_exchanges", "futures_exchanges", "fit_breakdown"):
            if isinstance(d.get(k), str):
                try:
                    d[k] = json.loads(d[k])
                except Exception:
                    pass
        res.append(d)

    return res, total


def get_stats(conn: sqlite3.Connection) -> dict:
    total = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    with_token = conn.execute(
        "SELECT COUNT(DISTINCT project_id) FROM tokens WHERE token_symbol != '' AND token_symbol IS NOT NULL"
    ).fetchone()[0]
    not_listed_gems = conn.execute(
        "SELECT COUNT(DISTINCT project_id) FROM tokens WHERE unlisted_gem_flag = 1"
    ).fetchone()[0]
    upbit_candidates = conn.execute(
        "SELECT COUNT(DISTINCT project_id) FROM tokens WHERE upbit_listed = 0 AND upbit_fit_score >= 70"
    ).fetchone()[0]
    last_scan = conn.execute(
        "SELECT * FROM scan_logs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()

    return {
        "total_projects": total,
        "with_token": with_token,
        "not_listed": not_listed_gems,
        "upbit_candidates": upbit_candidates,
        "last_scan": dict(last_scan) if last_scan else None,
    }


def delete_project(conn: sqlite3.Connection, project_id: int) -> bool:
    cur = conn.cursor()
    cur.execute("DELETE FROM tokens WHERE project_id = ?", (project_id,))
    cur.execute("DELETE FROM projects WHERE id = ?", (project_id,))
    conn.commit()
    return cur.rowcount > 0


def clear_all_projects(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("DELETE FROM tokens")
    cur.execute("DELETE FROM projects")
    conn.commit()


def get_scan_logs(conn: sqlite3.Connection, limit=20) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM scan_logs ORDER BY started_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


if __name__ == "__main__":
    init_db()
    print(f"数据库已创建/升级: {_DB_PATH}")
