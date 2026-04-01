"""
WickedSync — FastAPI application.

Endpoints:
  GET  /                      → Web UI
  GET  /api/status            → System stats
  GET  /api/jobs              → List all jobs
  POST /api/jobs              → Add a single job
  GET  /api/jobs/{id}         → Job detail + files
  POST /api/import/csv        → Import CSV and queue jobs
  POST /api/import/url        → Queue one URL (same as POST /api/jobs)
  POST /api/ingest            → Accept pre-resolved CDN URLs from browser scraper
  POST /api/worker/start      → Start download worker
  POST /api/worker/stop       → Stop download worker
  GET  /api/library/{cat}     → Browse NAS folder
  POST /api/chat              → Claude agent chat
  GET  /api/chat/history      → Recent chat messages
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import settings
from app import database as db
from app.downloader import worker
from app.claude_agent import ClaudeAgent, _discover_job
from app.folder_organizer import (
    build_organization_plan,
    execute_organization_plan,
    FolderMatcher,
    save_preference,
    load_preferences,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    worker.start()
    logger.info("WickedSync started ✓")
    yield
    worker.stop()
    logger.info("WickedSync stopped")


app = FastAPI(title="WickedSync", version="1.0.0", lifespan=lifespan)

# Allow cross-origin requests so the Claude-in-Chrome scraper can POST
# CDN URLs to this API from inside a Gumroad browser tab.
# This is a local-network service — broad CORS is fine here.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the static frontend
STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

_agent = ClaudeAgent()

# ── Request/Response models ───────────────────────────────────────────────────

class AddJobRequest(BaseModel):
    model_name: str
    term: str  # Movies | VG | Marvel | Wildcard
    product_url: str


class ImportCsvRequest(BaseModel):
    csv_text: str
    filter_year: int | None = None
    filter_month: str | None = None
    filter_term: str | None = None
    max_items: int = 9999


class ChatRequest(BaseModel):
    message: str


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the single-page web UI."""
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.isfile(index_path):
        with open(index_path) as f:
            return f.read()
    return HTMLResponse("<h1>WickedSync</h1><p>static/index.html not found.</p>")


# ── Status ────────────────────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    stats = await db.get_stats()
    stats["worker_running"] = worker._running
    stats["concurrent_limit"] = settings.concurrent_downloads
    stats["paths"] = {
        "movies": settings.movies_path,
        "vg": settings.vg_path,
        "marvel": settings.marvel_path,
    }
    return stats


# ── Jobs ──────────────────────────────────────────────────────────────────────

@app.get("/api/jobs")
async def list_jobs(status: str | None = None):
    return await db.list_jobs(status)


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: int):
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    files = await db.list_files(job_id)
    return {**job, "files": files}


@app.post("/api/jobs", status_code=201)
async def add_job(req: AddJobRequest):
    if await db.job_exists(req.product_url):
        return {"result": "skipped", "reason": "already exists"}

    job_id = await db.create_job(req.model_name, req.term, req.product_url)
    asyncio.create_task(_discover_job(job_id))
    return {"result": "queued", "job_id": job_id}


# ── Import ────────────────────────────────────────────────────────────────────

@app.post("/api/import/csv")
async def import_csv(req: ImportCsvRequest):
    """Queue jobs from a CSV string."""
    import csv as csvmod
    import io

    reader = csvmod.DictReader(io.StringIO(req.csv_text))
    queued = 0
    skipped = 0
    errors = []

    for row in reader:
        if queued >= req.max_items:
            break

        year = row.get("Year", "").strip()
        month = row.get("Month", "").strip()
        term = row.get("Term", "").strip()
        model_name = row.get("Model Name", "").strip()
        product_url = row.get("Gumroad URL", "").strip()

        if not product_url or not model_name:
            continue
        if req.filter_year and str(req.filter_year) != year:
            continue
        if req.filter_month and req.filter_month.lower() != month.lower():
            continue
        if req.filter_term and req.filter_term.lower() != term.lower():
            continue

        if await db.job_exists(product_url):
            skipped += 1
            continue

        try:
            job_id = await db.create_job(model_name, term, product_url)
            asyncio.create_task(_discover_job(job_id))
            queued += 1
        except Exception as e:
            errors.append(f"{model_name}: {e}")

    return {"queued": queued, "skipped_duplicates": skipped, "errors": errors[:20]}


@app.post("/api/import/url")
async def import_url(req: AddJobRequest):
    """Alias for add_job — convenient for single-URL API clients."""
    return await add_job(req)


# ── Browser scraper ingest ────────────────────────────────────────────────────
#
# This is the endpoint that Claude-in-Chrome calls after scraping CDN URLs
# from Gumroad product pages. No Gumroad auth needed on the NAS side —
# the browser already resolved the URLs (same public IP as NAS).
#
# Typical call from browser JS:
#   await fetch('http://192.168.1.168:8088/api/ingest', {
#     method: 'POST',
#     headers: {'Content-Type': 'application/json'},
#     body: JSON.stringify({ files: [...] })
#   })

class IngestFile(BaseModel):
    cdn_url: str                  # CloudFront or files.gumroad.com URL
    filename: str                 # e.g. "Wicked - Blade (Non Supported).zip"
    model_name: str               # e.g. "Blade Sculpture"
    term: str                     # Movies | VG | Marvel | Wildcard
    dest_path: str | None = None  # override destination; app derives it if None


class IngestRequest(BaseModel):
    files: list[IngestFile]
    # Optional: group all these files under a single named job
    job_name: str | None = None


@app.post("/api/ingest", status_code=201)
async def ingest_files(req: IngestRequest):
    """
    Accept pre-resolved CDN URLs from the Claude-in-Chrome browser scraper
    and queue them for immediate download.

    Unlike POST /api/jobs (which triggers Gumroad discovery via Playwright),
    this endpoint skips discovery entirely — the CDN URLs are already known.
    Files go straight into the download queue.
    """
    if not req.files:
        raise HTTPException(status_code=400, detail="No files provided")

    # Group by model_name so each product gets its own job row
    from collections import defaultdict
    by_model: dict[str, list[IngestFile]] = defaultdict(list)
    for f in req.files:
        by_model[f.model_name].append(f)

    created_jobs = []
    queued_files = 0

    for model_name, files in by_model.items():
        term = files[0].term
        # Use a synthetic "already-ingested" URL as the product_url key
        synthetic_url = f"ingest://{model_name.lower().replace(' ', '-')}"

        # Reuse existing job if one already exists for this model
        existing = await db.list_jobs()
        job_id = next(
            (j["id"] for j in existing if j["model_name"] == model_name and j["term"] == term),
            None,
        )

        if job_id is None:
            job_id = await db.create_job(model_name, term, synthetic_url)
            await db.update_job(job_id, status="queued", content_url=synthetic_url)

        base_path = settings.term_to_path.get(term, settings.movies_path)
        from app.organizer import dest_path_for_file, clean_filename

        for f in files:
            filename = clean_filename(f.filename)
            dest = f.dest_path or dest_path_for_file(base_path, filename)
            file_id = await db.create_file(
                job_id=job_id,
                filename=filename,
                cdn_url=f.cdn_url,
                dest_path=dest,
            )
            queued_files += 1

        # Update job file count
        file_count = len(await db.list_files(job_id))
        await db.update_job(job_id, file_count=file_count, status="queued")
        created_jobs.append({"job_id": job_id, "model_name": model_name, "files": len(files)})

    return {
        "result": "ingested",
        "jobs": created_jobs,
        "total_files_queued": queued_files,
        "message": f"Queued {queued_files} files across {len(created_jobs)} models. Worker will download automatically.",
    }


# ── Worker control ────────────────────────────────────────────────────────────

@app.post("/api/worker/start")
async def start_worker():
    if not worker._running:
        worker.start()
        return {"result": "started"}
    return {"result": "already running"}


@app.post("/api/worker/stop")
async def stop_worker():
    worker.stop()
    return {"result": "stopped"}


# ── Library browser ───────────────────────────────────────────────────────────

@app.get("/api/library/{category}")
async def list_library(category: str, search: str = ""):
    path_map = settings.term_to_path
    if category not in path_map:
        raise HTTPException(status_code=400, detail=f"Unknown category: {category}")

    base_path = path_map[category]
    if not os.path.isdir(base_path):
        return {"category": category, "path": base_path, "folders": [], "error": "Path not mounted"}

    folders = []
    for name in sorted(os.listdir(base_path)):
        if search and search.lower() not in name.lower():
            continue
        full = os.path.join(base_path, name)
        if os.path.isdir(full):
            zips = [f for f in os.listdir(full) if f.endswith(".zip")]
            folders.append({"name": name, "zip_count": len(zips)})

    return {"category": category, "path": base_path, "folder_count": len(folders), "folders": folders}


# ── Claude chat ───────────────────────────────────────────────────────────────

@app.post("/api/chat")
async def chat(req: ChatRequest):
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    # Save user message
    await db.save_message("user", req.message)

    # Build history for context (last 10 exchanges)
    history = await db.get_recent_messages(limit=20)
    # Exclude the message we just saved (it's already in history)
    messages = [{"role": m["role"], "content": m["content"]} for m in history[:-1]]

    # Get Claude's response
    reply = await _agent.chat(req.message, history=messages)

    # Save assistant message
    await db.save_message("assistant", reply)

    return {"reply": reply}


@app.get("/api/chat/history")
async def chat_history(limit: int = 50):
    return await db.get_recent_messages(limit=limit)


# ── Folder organization ───────────────────────────────────────────────────────

class OrganizeConfirmRequest(BaseModel):
    confirmed: bool = False


@app.get("/api/organize/{category}")
async def organize_plan(category: str):
    """
    Dry-run: return a plan of what would be moved, with fuzzy-match suggestions
    for files that don't cleanly resolve.
    """
    path_map = settings.term_to_path
    if category not in path_map:
        raise HTTPException(status_code=400, detail=f"Unknown category: {category}")

    base_path = path_map[category]
    plan = build_organization_plan(base_path)

    # Enrich moves with fuzzy-match confidence
    existing_folders = [
        name for name in (os.listdir(base_path) if os.path.isdir(base_path) else [])
        if os.path.isdir(os.path.join(base_path, name))
    ]
    prefs = await load_preferences()
    matcher = FolderMatcher(existing_folders)

    enriched_moves = []
    needs_review = []
    for move in plan["moves"]:
        stem = os.path.basename(move["src"])[:-4]
        # Check learned preferences first
        canonical = prefs.get(stem)
        if canonical:
            result = {"canonical": canonical, "confidence": 1.0, "action": "auto", "alternatives": []}
        else:
            result = matcher.match(stem)

        entry = {
            "src_filename": os.path.basename(move["src"]),
            "suggested_folder": result["canonical"],
            "confidence": result["confidence"],
            "action": result["action"],
            "alternatives": result["alternatives"],
        }
        enriched_moves.append(entry)
        if result["action"] in ("review", "ask"):
            needs_review.append(entry)

    return {
        "category": category,
        "base_path": base_path,
        "summary": plan["summary"],
        "needs_review": needs_review,
        "all_moves": enriched_moves,
        "empty_dirs": plan["empty_dirs_to_remove"],
        "conflict_folders": plan["conflict_renames"],
    }


@app.post("/api/organize/{category}")
async def organize_execute(category: str, req: OrganizeConfirmRequest):
    """Execute the organization plan for a category."""
    path_map = settings.term_to_path
    if category not in path_map:
        raise HTTPException(status_code=400, detail=f"Unknown category: {category}")

    base_path = path_map[category]
    plan = build_organization_plan(base_path)
    result = execute_organization_plan(plan, dry_run=not req.confirmed)
    return result


class SavePreferenceRequest(BaseModel):
    pattern: str    # zip stem (without .zip)
    canonical: str  # folder name to use


@app.post("/api/organize/preferences")
async def save_org_preference(req: SavePreferenceRequest):
    """Teach the app a folder mapping (pattern → canonical name)."""
    await save_preference(req.pattern, req.canonical)
    return {"saved": True, "pattern": req.pattern, "canonical": req.canonical}


@app.get("/api/organize/preferences")
async def list_org_preferences():
    """List all learned folder mappings."""
    prefs = await load_preferences()
    return {"count": len(prefs), "preferences": prefs}
