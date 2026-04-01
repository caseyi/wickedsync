"""
Integration tests for the FastAPI endpoints.
Uses httpx's AsyncClient with the test DB backed by a temp file.
No real Gumroad or NAS access is needed.
"""
import os
import pytest
import pytest_asyncio
import tempfile

# Point the DB at a temp file before importing the app
_tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _tmp_db.name
os.environ["ANTHROPIC_API_KEY"] = "test-key-not-real"
os.environ["GUMROAD_COOKIES"] = ""

from httpx import AsyncClient, ASGITransport
from app.main import app
from app import database as db


@pytest_asyncio.fixture(autouse=True)
async def init_db():
    """Re-init the DB before each test."""
    await db.init_db()
    yield
    # Clean up all tables between tests
    import aiosqlite
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        await conn.execute("DELETE FROM jobs")
        await conn.execute("DELETE FROM files")
        await conn.execute("DELETE FROM chat_messages")
        await conn.execute("DELETE FROM franchise_tags")
        await conn.execute("DELETE FROM folder_preferences")
        await conn.commit()


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ── /api/status ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_status_ok(client):
    res = await client.get("/api/status")
    assert res.status_code == 200
    data = res.json()
    assert "total_jobs" in data
    assert "worker_running" in data


# ── /api/jobs ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_jobs_empty(client):
    res = await client.get("/api/jobs")
    assert res.status_code == 200
    assert res.json() == []


@pytest.mark.asyncio
async def test_add_job(client):
    payload = {
        "model_name": "Blade Sculpture",
        "term": "Marvel",
        "product_url": "https://3dwicked.gumroad.com/l/BladeSculpture/test123",
    }
    res = await client.post("/api/jobs", json=payload)
    assert res.status_code == 201
    data = res.json()
    assert data["result"] == "queued"
    assert "job_id" in data


@pytest.mark.asyncio
async def test_add_job_duplicate_skipped(client):
    payload = {
        "model_name": "Blade Sculpture",
        "term": "Marvel",
        "product_url": "https://3dwicked.gumroad.com/l/BladeSculpture/test123",
    }
    await client.post("/api/jobs", json=payload)
    res = await client.post("/api/jobs", json=payload)
    assert res.status_code == 201
    assert res.json()["result"] == "skipped"


@pytest.mark.asyncio
async def test_get_job_not_found(client):
    res = await client.get("/api/jobs/99999")
    assert res.status_code == 404


@pytest.mark.asyncio
async def test_get_job_detail(client):
    payload = {
        "model_name": "Blade Sculpture",
        "term": "Marvel",
        "product_url": "https://3dwicked.gumroad.com/l/BladeSculpture/test456",
    }
    create_res = await client.post("/api/jobs", json=payload)
    job_id = create_res.json()["job_id"]

    res = await client.get(f"/api/jobs/{job_id}")
    assert res.status_code == 200
    data = res.json()
    assert data["model_name"] == "Blade Sculpture"
    assert data["term"] == "Marvel"
    assert "files" in data


# ── /api/import/csv ───────────────────────────────────────────────────────────

SAMPLE_CSV = """Year,Month,Term,Model Name,Type,Gumroad URL
2025,October,Marvel,Blade VS Dracula Diorama,Diorama,https://3dwicked.gumroad.com/l/BladeVSDraculaDiorama/ozsv5yo
2025,October,Marvel,Blade Sculpture,Sculpture,https://3dwicked.gumroad.com/l/BladeSculpture/w3g5s2s
2025,October,Movies,Chatterer Cenobite Sculpture,Sculpture,https://3dwicked.gumroad.com/l/ChattererS/yf059vt
2025,October,VG,Halo Diorama,Diorama,https://3dwicked.gumroad.com/l/HaloD/w1n46sl
"""

@pytest.mark.asyncio
async def test_import_csv_queues_all(client):
    res = await client.post("/api/import/csv", json={"csv_text": SAMPLE_CSV})
    assert res.status_code == 200
    data = res.json()
    assert data["queued"] == 4
    assert data["skipped_duplicates"] == 0


@pytest.mark.asyncio
async def test_import_csv_filter_by_term(client):
    res = await client.post("/api/import/csv", json={"csv_text": SAMPLE_CSV, "filter_term": "Marvel"})
    assert res.status_code == 200
    data = res.json()
    assert data["queued"] == 2  # Only the 2 Marvel rows


@pytest.mark.asyncio
async def test_import_csv_filter_by_month(client):
    res = await client.post("/api/import/csv", json={"csv_text": SAMPLE_CSV, "filter_month": "October"})
    assert res.status_code == 200
    assert res.json()["queued"] == 4


@pytest.mark.asyncio
async def test_import_csv_no_duplicates(client):
    await client.post("/api/import/csv", json={"csv_text": SAMPLE_CSV})
    res = await client.post("/api/import/csv", json={"csv_text": SAMPLE_CSV})
    data = res.json()
    assert data["queued"] == 0
    assert data["skipped_duplicates"] == 4


@pytest.mark.asyncio
async def test_import_csv_max_items(client):
    res = await client.post("/api/import/csv", json={"csv_text": SAMPLE_CSV, "max_items": 2})
    assert res.json()["queued"] == 2


# ── /api/worker ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_worker_start_stop(client):
    res = await client.post("/api/worker/start")
    assert res.status_code == 200

    res = await client.post("/api/worker/stop")
    assert res.status_code == 200
    assert res.json()["result"] == "stopped"


# ── /api/organize/preferences ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_save_and_list_preferences(client):
    res = await client.post("/api/organize/preferences", json={
        "pattern": "WICKED - Jack And Sally Diorama",
        "canonical": "WICKED - Jack and Sally Diorama",
    })
    assert res.status_code == 200

    res = await client.get("/api/organize/preferences")
    data = res.json()
    assert data["count"] >= 1
    assert data["preferences"]["WICKED - Jack And Sally Diorama"] == "WICKED - Jack and Sally Diorama"


# ── / (web UI) ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_index_returns_html(client):
    res = await client.get("/")
    assert res.status_code == 200
    assert "WickedSync" in res.text
