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

from app.config import settings, CODENAME
from app import database as db
from app.downloader import worker
from app.claude_agent import ClaudeAgent, _discover_job
from app.folder_organizer import (
    build_organization_plan,
    execute_organization_plan,
    scan_library_health,
    redistribute_term_bucket,
    delete_empty_folders,
    FolderMatcher,
    save_preference,
    load_preferences,
)
import anthropic as _anthropic

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
    from app.downloader import get_concurrency
    stats = await db.get_stats()
    stats["worker_running"] = worker._running
    stats["concurrent_limit"] = get_concurrency()
    stats["codename"] = CODENAME
    stats["paths"] = {
        "movies": settings.movies_path,
        "vg": settings.vg_path,
        "marvel": settings.marvel_path,
    }
    return stats


# ── Jobs ──────────────────────────────────────────────────────────────────────

@app.get("/api/jobs")
async def list_jobs(status: str | None = None):
    return await db.get_jobs_with_files(status)


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: int):
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    files = await db.list_files(job_id)
    return {**job, "files": files}


@app.get("/api/jobs/stats")
async def jobs_stats():
    """Return per-category counts and a breakdown of statuses."""
    jobs = await db.list_jobs()
    by_term: dict[str, dict] = {}
    status_counts: dict[str, int] = {}
    total_size = 0

    for j in jobs:
        term = j["term"]
        if term not in by_term:
            by_term[term] = {"total": 0, "done": 0, "error": 0, "pending": 0, "downloading": 0}
        by_term[term]["total"] += 1
        st = j["status"]
        by_term[term][st] = by_term[term].get(st, 0) + 1
        status_counts[st] = status_counts.get(st, 0) + 1

    # Sum completed file sizes
    all_files = await db.get_all_done_files()
    total_size = sum(f.get("size_bytes", 0) or 0 for f in all_files)

    return {
        "total_jobs": len(jobs),
        "by_term": by_term,
        "by_status": status_counts,
        "done_gb": round(total_size / 1e9, 2),
        "error_count": status_counts.get("error", 0),
    }


@app.post("/api/jobs/retry-errors")
async def retry_errors():
    """Reset all error-status jobs back to pending and re-queue discovery."""
    error_jobs = await db.list_jobs(status="error")
    if not error_jobs:
        return {"retried": 0, "message": "No error jobs found."}

    retried = 0
    for job in error_jobs:
        await db.update_job(job["id"], status="pending", error_msg=None)
        asyncio.create_task(_discover_job(job["id"]))
        retried += 1

    # Auto-start worker
    if not worker._running:
        worker.start()

    return {"retried": retried, "message": f"Re-queued {retried} jobs."}


@app.post("/api/jobs", status_code=201)
async def add_job(req: AddJobRequest):
    if await db.job_exists(req.product_url):
        return {"result": "skipped", "reason": "already exists"}

    job_id = await db.create_job(req.model_name, req.term, req.product_url)
    asyncio.create_task(_discover_job(job_id))

    # Auto-start worker if idle
    if not worker._running:
        worker.start()
        logger.info("Worker auto-started on new job.")

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

    # Auto-start worker if idle and we queued something
    if queued > 0 and not worker._running:
        worker.start()
        logger.info(f"Worker auto-started after CSV import of {queued} jobs.")

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


class ConcurrencyRequest(BaseModel):
    limit: int


@app.post("/api/worker/concurrency")
async def set_worker_concurrency(req: ConcurrencyRequest):
    """Adjust the number of simultaneous downloads at runtime (min 1, max 20)."""
    from app.downloader import set_concurrency, get_concurrency
    clamped = max(1, min(20, req.limit))
    set_concurrency(clamped)
    return {"concurrent_limit": clamped}


@app.get("/api/worker/concurrency")
async def get_worker_concurrency():
    from app.downloader import get_concurrency
    return {"concurrent_limit": get_concurrency()}


# ── Library Snapshot (must be before /{category} to avoid route shadowing) ────

@app.get("/api/library/snapshot")
async def library_snapshot(format: str = "txt"):
    """
    Export a full snapshot of all mounted library folders.
    format=txt  → plain text optimised for pasting into Claude
    format=json → machine-readable dict
    """
    from datetime import datetime as _dt

    categories = {}
    for cat, path in settings.term_to_path.items():
        if not os.path.isdir(path) or cat == "Wildcard":
            continue
        folders = []
        for name in sorted(os.listdir(path)):
            full = os.path.join(path, name)
            if not os.path.isdir(full):
                continue
            try:
                zips = sum(1 for f in os.listdir(full) if f.lower().endswith(".zip"))
            except PermissionError:
                zips = -1
            folders.append({"name": name, "zips": zips})
        categories[cat] = folders

    if format == "json":
        return {"generated": _dt.utcnow().isoformat(), "categories": categories}

    lines = [
        f"# WickedSync Library Snapshot — {_dt.utcnow().strftime('%Y-%m-%d')}",
        "#",
        "# Categories: " + ", ".join(
            f"{cat} ({len(fols)} folders)" for cat, fols in categories.items()
        ),
        "#",
        "# ── INSTRUCTIONS FOR CLAUDE ────────────────────────────────────────────",
        "# Review the folder list below and respond ONLY with annotation lines.",
        "# Use these exact formats (one per line, no extra text):",
        "#",
        "#   FRANCHISE: <folder_name> → <Franchise Name>",
        "#     (group this folder under a franchise sub-directory)",
        "#",
        "#   RENAME: <old_name> → <new_name>",
        "#     (fix typo / normalise capitalisation)",
        "#",
        "#   MERGE: <folder_a> + <folder_b>",
        "#     (these look like duplicates — flag for review)",
        "#",
        "#   TAG: <folder_name> | term=<Movies|VG|Marvel>",
        "#     (re-categorise to a different term)",
        "#",
        "# Omit folders that need no changes.  Do not explain your reasoning.",
        "# ────────────────────────────────────────────────────────────────────────",
        "",
    ]

    for cat, folders in categories.items():
        lines.append(f"[{cat}]  ({len(folders)} folders)")
        for f in folders:
            zip_label = f"  ({f['zips']} zips)" if f["zips"] >= 0 else ""
            lines.append(f"  {f['name']}{zip_label}")
        lines.append("")

    return "\n".join(lines)


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


# ── Franchise tags ────────────────────────────────────────────────────────────

class FranchiseTagRequest(BaseModel):
    model_name: str
    franchise: str
    term: str = ""
    source: str = "user"


class FranchiseSuggestRequest(BaseModel):
    model_names: list[str] | None = None   # if None, use all known job model_names
    term: str | None = None                # filter by term


@app.get("/api/tags/library-overview")
async def tags_library_overview():
    """
    Return every folder from every mounted category, merged with franchise tag status.
    Used by the Tags tab to show all folders (tagged + untagged).
    """
    tag_rows = await db.list_franchise_tags()
    franchise_map = {r["model_name"]: r for r in tag_rows}
    result = []
    for cat, path in settings.term_to_path.items():
        if not os.path.isdir(path):
            continue
        try:
            entries = sorted(os.listdir(path))
        except PermissionError:
            continue
        for name in entries:
            if not os.path.isdir(os.path.join(path, name)):
                continue
            tag = franchise_map.get(name)
            result.append({
                "model_name": name,
                "category": cat,
                "franchise": tag["franchise"] if tag else None,
                "term": tag["term"] if tag else cat,
                "source": tag["source"] if tag else None,
            })
    result.sort(key=lambda x: (x["franchise"] is None, x["model_name"].lower()))
    return {"count": len(result), "folders": result}


@app.get("/api/tags")
async def list_tags():
    """Return all saved franchise tags."""
    tags = await db.list_franchise_tags()
    return {"count": len(tags), "tags": tags}


@app.post("/api/tags", status_code=201)
async def save_tag(req: FranchiseTagRequest):
    """Save or update a franchise tag for a model."""
    await db.set_franchise_tag(req.model_name, req.franchise, req.term, req.source)
    return {"saved": True, "model_name": req.model_name, "franchise": req.franchise}


@app.delete("/api/tags/{model_name}")
async def delete_tag(model_name: str):
    """Remove a franchise tag."""
    await db.delete_franchise_tag(model_name)
    return {"deleted": True, "model_name": model_name}


@app.post("/api/tags/suggest")
async def suggest_franchise_tags(req: FranchiseSuggestRequest):
    """
    Ask Claude to group a list of model names into franchise/universe buckets.
    Returns suggested {model_name: franchise} mappings without saving them.
    Pass model_names=null to auto-pull all known job model names from the DB.
    """
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    # Collect model names
    if req.model_names:
        names = req.model_names
    else:
        jobs = await db.list_jobs()
        if req.term:
            jobs = [j for j in jobs if j.get("term", "").lower() == req.term.lower()]
        names = sorted({j["model_name"] for j in jobs})

    if not names:
        return {"suggestions": {}, "message": "No model names found."}

    # Ask Claude to group them
    prompt = (
        "You are a 3D printing STL library organiser. Below is a list of 3D model names "
        "from a Gumroad library. Group each model into its franchise, movie, game, or universe. "
        "Return ONLY valid JSON: an object where each key is a model name from the list "
        "and the value is the franchise/universe name (e.g. 'Lord of the Rings', 'Marvel', "
        "'Star Wars', 'DC Comics', 'Video Games', etc). "
        "Use 'Standalone' for models that don't belong to any franchise. "
        "Keep franchise names consistent — if two models are from the same franchise use the "
        "exact same string. Output only the JSON object, no explanation.\n\n"
        "Model names:\n" + "\n".join(f"- {n}" for n in names)
    )

    client = _anthropic.Anthropic(api_key=settings.anthropic_api_key)
    message = client.messages.create(
        model=settings.claude_model,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()
    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = "\n".join(raw.split("\n")[1:])
        raw = raw.rstrip("`").strip()

    import json
    try:
        suggestions = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=500, detail=f"Claude returned unparseable JSON: {raw[:300]}")

    return {"suggestions": suggestions, "model_count": len(suggestions)}


@app.post("/api/tags/apply-suggestions")
async def apply_suggestions(body: dict):
    """
    Save a {model_name: franchise} dict as Claude-sourced franchise tags.
    Body: {"suggestions": {"Model A": "Franchise X", ...}}
    """
    suggestions: dict = body.get("suggestions", {})
    saved = 0
    for model_name, franchise in suggestions.items():
        if franchise and franchise.lower() != "standalone":
            await db.set_franchise_tag(model_name, franchise, source="claude")
            saved += 1
    return {"saved": saved}


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
    custom_path: str | None = None  # override the category's default path


def _resolve_org_path(category: str, custom_path: str | None) -> str:
    """Return the base directory to scan, honouring custom_path if given."""
    if custom_path:
        return custom_path
    path_map = settings.term_to_path
    if category not in path_map:
        raise HTTPException(status_code=400, detail=f"Unknown category: {category}")
    return path_map[category]


async def _build_enriched_plan(base_path: str, category: str = "") -> dict:
    """Scan base_path and return an enriched plan with fuzzy-match confidence."""
    franchise_map = await db.get_franchise_tags()
    plan = build_organization_plan(base_path, franchise_map=franchise_map)

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
        canonical = prefs.get(stem)
        if canonical:
            result = {"canonical": canonical, "confidence": 1.0, "action": "auto", "alternatives": []}
        else:
            result = matcher.match(stem)

        entry = {
            "src_filename": os.path.basename(move["src"]),
            "src_path": move["src"],
            "suggested_folder": result["canonical"],
            "dest_path": move["dest"],
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


@app.get("/api/organize")
async def list_organize_targets():
    """Return all available category targets with their paths and mount status."""
    path_map = settings.term_to_path
    targets = []
    for cat, path in path_map.items():
        mounted = os.path.isdir(path)
        folder_count = 0
        loose_zips = 0
        if mounted:
            entries = os.listdir(path)
            folder_count = sum(1 for e in entries if os.path.isdir(os.path.join(path, e)))
            loose_zips = sum(1 for e in entries if e.lower().endswith(".zip"))
        targets.append({
            "category": cat,
            "path": path,
            "mounted": mounted,
            "folder_count": folder_count,
            "loose_zips": loose_zips,
        })
    return {"targets": targets}


@app.get("/api/organize/browse")
async def browse_directory(path: str = ""):
    """
    List subdirectories of a given path (for the folder picker).
    Defaults to listing the top-level category roots.
    """
    if not path:
        # Return root-level category paths
        return {"path": "/", "dirs": list(settings.term_to_path.values())}

    if not os.path.isdir(path):
        raise HTTPException(status_code=404, detail=f"Directory not found: {path}")

    try:
        entries = sorted(os.listdir(path))
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")

    dirs = []
    for name in entries:
        full = os.path.join(path, name)
        if os.path.isdir(full):
            zip_count = sum(1 for f in os.listdir(full) if f.lower().endswith(".zip"))
            dirs.append({"name": name, "path": full, "zip_count": zip_count})

    return {"path": path, "dirs": dirs}


@app.get("/api/organize/preferences")
async def list_org_preferences():
    """List all learned folder mappings."""
    prefs = await load_preferences()
    return {"count": len(prefs), "preferences": prefs}


class SavePreferenceRequest(BaseModel):
    pattern: str    # zip stem (without .zip)
    canonical: str  # folder name to use


# NOTE: specific routes (/preferences) MUST be registered before the
# parameterized route (/{category}) so FastAPI doesn't swallow them.
@app.post("/api/organize/preferences")
async def save_org_preference(req: SavePreferenceRequest):
    """Teach the app a folder mapping (pattern → canonical name)."""
    await save_preference(req.pattern, req.canonical)
    return {"saved": True, "pattern": req.pattern, "canonical": req.canonical}


@app.get("/api/organize/{category}")
async def organize_plan(category: str, custom_path: str | None = None):
    """
    Dry-run: return a full plan of what would be moved, with fuzzy-match confidence
    for every file. Optionally override the scan path with custom_path.
    """
    base_path = _resolve_org_path(category, custom_path)
    return await _build_enriched_plan(base_path, category)


@app.post("/api/organize/{category}")
async def organize_execute(category: str, req: OrganizeConfirmRequest):
    """Execute the organization plan for a category (or custom path)."""
    base_path = _resolve_org_path(category, req.custom_path)
    franchise_map = await db.get_franchise_tags()
    plan = build_organization_plan(base_path, franchise_map=franchise_map)
    result = execute_organization_plan(plan, dry_run=not req.confirmed)
    return result


# ── Library Health ────────────────────────────────────────────────────────────

class HealthRedistributeRequest(BaseModel):
    bucket_path: str
    base_dir: str
    confirmed: bool = False


class HealthDeleteEmptyRequest(BaseModel):
    folder_paths: list[str]
    confirmed: bool = False


class HealthRenameRequest(BaseModel):
    folder_names: list[str] | None = None  # None → use all folders in category
    category: str | None = None
    custom_path: str | None = None


class HealthRenameApplyRequest(BaseModel):
    base_dir: str
    renames: list[dict]   # [{"original": str, "suggested": str}]
    confirmed: bool = False


@app.get("/api/organize/health/{category}")
async def library_health(category: str, custom_path: str | None = None):
    """
    Run a full health scan on a category (or custom path).
    Returns: term buckets, fuzzy duplicates, empty folders, cross-category dupes.
    """
    base_path = _resolve_org_path(category, custom_path)

    # Build cross-category dirs for cross-cat dupe detection
    cross_dirs = {k: v for k, v in settings.term_to_path.items() if os.path.isdir(v)}

    import asyncio as _asyncio
    loop = _asyncio.get_event_loop()
    franchise_map = await db.get_franchise_tags()
    report = await loop.run_in_executor(
        None, lambda: scan_library_health(base_path, cross_category_dirs=cross_dirs, franchise_map=franchise_map)
    )
    report["category"] = category
    return report


@app.post("/api/organize/health/redistribute")
async def health_redistribute(req: HealthRedistributeRequest):
    """
    Move zips from a term bucket folder into their canonical model folders.
    Set confirmed=True to actually move files (default is dry-run preview).
    """
    import asyncio as _asyncio
    loop = _asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: redistribute_term_bucket(req.bucket_path, req.base_dir, dry_run=not req.confirmed),
    )
    return result


@app.post("/api/organize/health/delete-empty")
async def health_delete_empty(req: HealthDeleteEmptyRequest):
    """
    Delete a list of empty folders.
    Set confirmed=True to actually delete (default is dry-run preview).
    """
    import asyncio as _asyncio
    loop = _asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: delete_empty_folders(req.folder_paths, dry_run=not req.confirmed),
    )
    return result


@app.post("/api/organize/health/suggest-renames")
async def health_suggest_renames(req: HealthRenameRequest):
    """
    Ask Claude to suggest normalized folder names — fixing typos, capitalization,
    punctuation inconsistencies. Returns [{original, suggested, reason}] list
    WITHOUT making any file system changes.
    """
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    # Collect folder names
    if req.folder_names:
        names = req.folder_names
    else:
        base_path = _resolve_org_path(req.category or "Movies", req.custom_path)
        if not os.path.isdir(base_path):
            raise HTTPException(status_code=404, detail="Path not found")
        names = sorted([
            n for n in os.listdir(base_path)
            if os.path.isdir(os.path.join(base_path, n))
        ])

    if not names:
        return {"suggestions": [], "message": "No folders found."}

    prompt = (
        "You are a 3D printing STL library organiser. Below is a list of folder names "
        "from a library. Your job is to identify folders with naming issues such as:\n"
        "- Obvious typos (e.g. 'Barlog' instead of 'Balrog', 'Steet Fighter' instead of 'Street Fighter')\n"
        "- Case inconsistencies (e.g. 'WICKED - Batman' vs 'Wicked - Batman')\n"
        "- Punctuation errors or extra spaces\n"
        "- Misspellings that are clearly wrong\n\n"
        "Return ONLY valid JSON: an array of objects with keys "
        '"original" (exact input), "suggested" (corrected name), "reason" (brief explanation). '
        "Only include folders that need a change. If a folder name is correct, omit it. "
        "Output only the JSON array, no explanation.\n\n"
        "Folder names:\n" + "\n".join(f"- {n}" for n in names)
    )

    import json as _json

    client = _anthropic.Anthropic(api_key=settings.anthropic_api_key)
    message = client.messages.create(
        model=settings.claude_model,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()
    if raw.startswith("```"):
        raw = "\n".join(raw.split("\n")[1:]).rstrip("`").strip()

    try:
        suggestions = _json.loads(raw)
    except Exception:
        raise HTTPException(status_code=500, detail=f"Claude returned unparseable JSON: {raw[:300]}")

    return {"suggestions": suggestions, "folder_count": len(names), "suggestion_count": len(suggestions)}


@app.post("/api/organize/health/apply-renames")
async def health_apply_renames(req: HealthRenameApplyRequest):
    """
    Apply folder renames on disk.
    Set confirmed=True to actually rename (default is dry-run preview showing what would change).
    """
    results = []
    errors = []

    for rename in req.renames:
        original = rename.get("original", "").strip()
        suggested = rename.get("suggested", "").strip()
        if not original or not suggested or original == suggested:
            continue

        src = os.path.join(req.base_dir, original)
        dest = os.path.join(req.base_dir, suggested)

        if not os.path.isdir(src):
            errors.append({"original": original, "error": "Source folder not found"})
            continue

        if os.path.exists(dest):
            errors.append({"original": original, "error": f"Destination already exists: {suggested}"})
            continue

        if req.confirmed:
            try:
                import shutil as _shutil
                _shutil.move(src, dest)
                results.append({"original": original, "suggested": suggested, "status": "renamed"})
            except Exception as e:
                errors.append({"original": original, "error": str(e)})
        else:
            results.append({"original": original, "suggested": suggested, "status": "preview"})

    return {
        "dry_run": not req.confirmed,
        "results": results,
        "errors": errors,
        "summary": {"renamed": len(results), "errors": len(errors)},
    }


class ImportAnnotationsRequest(BaseModel):
    text: str           # the annotated text from Claude
    confirmed: bool = False   # False = preview only, True = apply


@app.post("/api/library/import-annotations")
async def import_annotations(req: ImportAnnotationsRequest):
    """
    Parse Claude's annotation output and apply / preview the changes.

    Supported directives:
      FRANCHISE: folder_name → Franchise Name
      RENAME:    old → new
      MERGE:     a + b          (preview-only, never auto-merged)
      TAG:       folder | term=Movies
    """
    import re as _re

    franchises_to_save = []   # (folder_name, franchise)
    renames_to_apply   = []   # (old, new)
    merges_flagged     = []   # (a, b)
    tags_to_save       = []   # (folder_name, term)
    parse_errors       = []

    for raw_line in req.text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.upper().startswith("FRANCHISE:"):
            body = line[10:].strip()
            m = _re.match(r"^(.+?)\s*(?:→|->|>)\s*(.+)$", body)
            if m:
                franchises_to_save.append((m.group(1).strip(), m.group(2).strip()))
            else:
                parse_errors.append(f"Bad FRANCHISE line: {line}")

        elif line.upper().startswith("RENAME:"):
            body = line[7:].strip()
            m = _re.match(r"^(.+?)\s*(?:→|->|>)\s*(.+)$", body)
            if m:
                renames_to_apply.append((m.group(1).strip(), m.group(2).strip()))
            else:
                parse_errors.append(f"Bad RENAME line: {line}")

        elif line.upper().startswith("MERGE:"):
            body = line[6:].strip()
            m = _re.match(r"^(.+?)\s*\+\s*(.+)$", body)
            if m:
                merges_flagged.append((m.group(1).strip(), m.group(2).strip()))
            else:
                parse_errors.append(f"Bad MERGE line: {line}")

        elif line.upper().startswith("TAG:"):
            body = line[4:].strip()
            m = _re.match(r"^(.+?)\s*\|\s*term=(\w+)$", body)
            if m:
                tags_to_save.append((m.group(1).strip(), m.group(2).strip()))
            else:
                parse_errors.append(f"Bad TAG line: {line}")

    applied = {"franchises": 0, "renames": 0, "tags": 0}

    if req.confirmed:
        # Apply franchise tags
        for folder_name, franchise in franchises_to_save:
            await db.set_franchise_tag(folder_name, franchise, source="annotation")
            applied["franchises"] += 1

        # Apply TAG recategorisations (save as franchise tag with term metadata)
        for folder_name, term in tags_to_save:
            await db.set_franchise_tag(folder_name, franchise=folder_name, term=term, source="annotation")
            applied["tags"] += 1

        # Apply renames (find which category each folder lives in)
        rename_results = []
        rename_errors = []
        for old, new in renames_to_apply:
            applied_rename = False
            for cat, base_path in settings.term_to_path.items():
                src = os.path.join(base_path, old)
                dest = os.path.join(base_path, new)
                if os.path.isdir(src):
                    if os.path.exists(dest):
                        rename_errors.append({"old": old, "error": f"Destination '{new}' already exists in {cat}"})
                    else:
                        try:
                            import shutil as _shutil
                            _shutil.move(src, dest)
                            rename_results.append({"old": old, "new": new, "category": cat})
                            applied["renames"] += 1
                            applied_rename = True
                        except Exception as e:
                            rename_errors.append({"old": old, "error": str(e)})
                    break
            if not applied_rename and not any(r["old"] == old for r in rename_errors):
                rename_errors.append({"old": old, "error": "Folder not found in any category"})
    else:
        rename_results = [{"old": o, "new": n} for o, n in renames_to_apply]
        rename_errors = []

    return {
        "dry_run": not req.confirmed,
        "parsed": {
            "franchises": [{"folder": f, "franchise": fr} for f, fr in franchises_to_save],
            "renames": [{"old": o, "new": n} for o, n in renames_to_apply],
            "merges": [{"a": a, "b": b} for a, b in merges_flagged],
            "tags": [{"folder": f, "term": t} for f, t in tags_to_save],
        },
        "applied": applied if req.confirmed else None,
        "rename_results": rename_results,
        "rename_errors": rename_errors,
        "parse_errors": parse_errors,
        "summary": {
            "franchises": len(franchises_to_save),
            "renames": len(renames_to_apply),
            "merges": len(merges_flagged),
            "tags": len(tags_to_save),
            "parse_errors": len(parse_errors),
        },
    }
