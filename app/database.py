"""SQLite database setup and helpers using aiosqlite."""
import json
import aiosqlite
from datetime import datetime
from typing import Optional

from app.config import settings

DB_PATH = settings.db_path

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS franchise_tags (
    model_name  TEXT PRIMARY KEY,
    franchise   TEXT NOT NULL,
    term        TEXT DEFAULT '',
    source      TEXT DEFAULT 'user',   -- 'user' | 'claude'
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name  TEXT NOT NULL,
    term        TEXT NOT NULL,          -- Movies, VG, Marvel, Wildcard
    product_url TEXT NOT NULL,          -- gumroad.com/l/... or gumroad.com/d/...
    content_url TEXT,                   -- gumroad.com/d/[hash]  (resolved)
    status      TEXT NOT NULL DEFAULT 'pending',
    -- pending → discovering → queued → downloading → done | error
    file_count  INTEGER DEFAULT 0,
    done_count  INTEGER DEFAULT 0,
    error_msg   TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      INTEGER NOT NULL REFERENCES jobs(id),
    filename    TEXT NOT NULL,
    cdn_url     TEXT,
    dest_path   TEXT,
    size_bytes  INTEGER DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'pending',
    -- pending → downloading → done | error
    error_msg   TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    role       TEXT NOT NULL,   -- user | assistant
    content    TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_TABLES)
        await db.commit()


def now() -> str:
    return datetime.utcnow().isoformat()


# ── Jobs ──────────────────────────────────────────────────────────────────────

async def create_job(model_name: str, term: str, product_url: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO jobs (model_name,term,product_url,status,created_at,updated_at) VALUES (?,?,?,?,?,?)",
            (model_name, term, product_url, "pending", now(), now()),
        )
        await db.commit()
        return cur.lastrowid


async def update_job(job_id: int, **kwargs):
    kwargs["updated_at"] = now()
    sets = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [job_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE jobs SET {sets} WHERE id=?", vals)
        await db.commit()


async def get_job(job_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def list_jobs(status: Optional[str] = None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if status:
            async with db.execute("SELECT * FROM jobs WHERE status=? ORDER BY created_at DESC", (status,)) as cur:
                return [dict(r) for r in await cur.fetchall()]
        async with db.execute("SELECT * FROM jobs ORDER BY created_at DESC") as cur:
            return [dict(r) for r in await cur.fetchall()]


async def job_exists(product_url: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id FROM jobs WHERE product_url=?", (product_url,)) as cur:
            return await cur.fetchone() is not None


# ── Files ─────────────────────────────────────────────────────────────────────

async def create_file(job_id: int, filename: str, cdn_url: str, dest_path: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO files (job_id,filename,cdn_url,dest_path,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
            (job_id, filename, cdn_url, dest_path, "pending", now(), now()),
        )
        await db.commit()
        return cur.lastrowid


async def update_file(file_id: int, **kwargs):
    kwargs["updated_at"] = now()
    sets = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [file_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE files SET {sets} WHERE id=?", vals)
        await db.commit()


async def list_files(job_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM files WHERE job_id=? ORDER BY filename", (job_id,)) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_pending_files() -> list[dict]:
    """Return all files with status='pending' that have a cdn_url ready."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM files WHERE status='pending' AND cdn_url IS NOT NULL ORDER BY created_at"
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


# ── Chat ──────────────────────────────────────────────────────────────────────

async def save_message(role: str, content: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO chat_messages (role,content,created_at) VALUES (?,?,?)",
            (role, content, now()),
        )
        await db.commit()


async def get_recent_messages(limit: int = 20) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM chat_messages ORDER BY created_at DESC LIMIT ?", (limit,)
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in reversed(rows)]


# ── Franchise tags ────────────────────────────────────────────────────────────

async def get_franchise_tags() -> dict[str, str]:
    """Return {model_name: franchise} for all saved tags."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT model_name, franchise FROM franchise_tags ORDER BY franchise, model_name") as cur:
            return {r["model_name"]: r["franchise"] for r in await cur.fetchall()}


async def list_franchise_tags() -> list[dict]:
    """Return all franchise tag rows as dicts."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM franchise_tags ORDER BY franchise, model_name") as cur:
            return [dict(r) for r in await cur.fetchall()]


async def set_franchise_tag(model_name: str, franchise: str, term: str = "", source: str = "user"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO franchise_tags (model_name, franchise, term, source, created_at, updated_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(model_name) DO UPDATE SET franchise=excluded.franchise,
                 term=excluded.term, source=excluded.source, updated_at=excluded.updated_at""",
            (model_name, franchise, term, source, now(), now()),
        )
        await db.commit()


async def delete_franchise_tag(model_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM franchise_tags WHERE model_name=?", (model_name,))
        await db.commit()


# ── Stats ─────────────────────────────────────────────────────────────────────

async def get_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM jobs") as cur:
            total_jobs = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM jobs WHERE status='done'") as cur:
            done_jobs = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM files") as cur:
            total_files = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM files WHERE status='done'") as cur:
            done_files = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM files WHERE status='downloading'") as cur:
            active_files = (await cur.fetchone())[0]
        async with db.execute("SELECT COALESCE(SUM(size_bytes),0) FROM files WHERE status='done'") as cur:
            total_bytes = (await cur.fetchone())[0]

    return {
        "total_jobs": total_jobs,
        "done_jobs": done_jobs,
        "total_files": total_files,
        "done_files": done_files,
        "active_files": active_files,
        "total_gb": round(total_bytes / 1e9, 2),
    }
