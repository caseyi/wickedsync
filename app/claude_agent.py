"""
Claude agent with tool use for natural language control of WickedSync.

Tools available to Claude:
  - get_status         : system health + download stats
  - list_jobs          : browse all jobs / filter by status
  - add_job            : queue a product for download
  - import_csv         : parse a 3DWicked CSV and queue all items
  - list_files         : show files in a job
  - list_library       : browse the NAS library folder
  - plan_organize      : preview what organize_folder would move (dry-run)
  - organize_folder    : actually reorganize files in a NAS category folder
  - start_worker       : start the download worker
  - stop_worker        : stop the download worker

Usage:
  agent = ClaudeAgent()
  reply = await agent.chat("Download all the October 2025 Marvel items")
"""
import asyncio
import csv
import io
import json
import logging
import os
from typing import Any

import anthropic

from app.config import settings
from app import database as db
from app.downloader import worker
from app.organizer import derive_folder_name

logger = logging.getLogger(__name__)

# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "get_status",
        "description": "Get system health: download stats, queue size, active downloads, and storage used.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "list_jobs",
        "description": "List download jobs, optionally filtered by status (pending/discovering/queued/downloading/done/error).",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "Filter by status. Leave empty for all jobs."},
                "limit": {"type": "integer", "description": "Max results to return. Default 50."},
            },
            "required": [],
        },
    },
    {
        "name": "add_job",
        "description": "Queue a single Gumroad product for download. Provide the product URL (l/ or d/ URL) and metadata.",
        "input_schema": {
            "type": "object",
            "properties": {
                "model_name": {"type": "string", "description": "Human-readable model name, e.g. 'Blade Sculpture'"},
                "term": {"type": "string", "description": "Category: Movies, VG, Marvel, or Wildcard"},
                "product_url": {"type": "string", "description": "Gumroad URL (l/slug/code or d/hash)"},
            },
            "required": ["model_name", "term", "product_url"],
        },
    },
    {
        "name": "import_csv",
        "description": (
            "Parse a 3DWicked missing-downloads CSV (columns: Year,Month,Term,Model Name,Type,Gumroad URL) "
            "and queue all items. Optionally filter by month, year, or term."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "csv_text": {"type": "string", "description": "Full CSV content as a string"},
                "filter_year": {"type": "integer", "description": "Only import items from this year"},
                "filter_month": {"type": "string", "description": "Only import items from this month name (e.g. 'October')"},
                "filter_term": {"type": "string", "description": "Only import items with this term (Movies/VG/Marvel/Wildcard)"},
                "max_items": {"type": "integer", "description": "Limit number of items to queue (useful for batching)"},
            },
            "required": ["csv_text"],
        },
    },
    {
        "name": "list_files",
        "description": "List individual files in a job, showing download status and filename.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "integer", "description": "Job ID to inspect"},
            },
            "required": ["job_id"],
        },
    },
    {
        "name": "list_library",
        "description": "Browse the NAS library folder to see what's already downloaded.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Category to list: Movies, VG, or Marvel",
                    "enum": ["Movies", "VG", "Marvel"],
                },
                "search": {"type": "string", "description": "Optional search string to filter folder names"},
            },
            "required": ["category"],
        },
    },
    {
        "name": "start_worker",
        "description": "Start the background download worker if it is not running.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "stop_worker",
        "description": "Pause/stop the background download worker.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "plan_organize",
        "description": (
            "Preview (dry-run) what organizing a NAS category folder would do: "
            "which files would be moved and where. Does NOT move anything. "
            "Use this before organize_folder to review changes with the user."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Category to organize: Movies, VG, or Marvel",
                    "enum": ["Movies", "VG", "Marvel"],
                },
            },
            "required": ["category"],
        },
    },
    {
        "name": "organize_folder",
        "description": (
            "Organize (move) zip files in a NAS category folder into per-model subfolders. "
            "Groups all variants (Non Supported, Pre Supported, Images, etc.) together. "
            "If confirmed=false (default), returns a preview without moving. "
            "Set confirmed=true only after the user has approved the plan."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Category to organize: Movies, VG, or Marvel",
                    "enum": ["Movies", "VG", "Marvel"],
                },
                "confirmed": {
                    "type": "boolean",
                    "description": "Set to true to actually move files. Default false (preview only).",
                },
            },
            "required": ["category"],
        },
    },
]

# ── Tool implementations ───────────────────────────────────────────────────────

async def _tool_get_status(_: dict) -> dict:
    stats = await db.get_stats()
    stats["worker_running"] = worker._running
    stats["concurrent_limit"] = settings.concurrent_downloads
    return stats


async def _tool_list_jobs(args: dict) -> list:
    status = args.get("status") or None
    limit = args.get("limit", 50)
    jobs = await db.list_jobs(status)
    return jobs[:limit]


async def _tool_add_job(args: dict) -> dict:
    model_name = args["model_name"]
    term = args["term"]
    product_url = args["product_url"]

    if await db.job_exists(product_url):
        return {"result": "skipped", "reason": "job already exists for this URL"}

    job_id = await db.create_job(model_name, term, product_url)

    # Kick off the discovery task in the background
    import asyncio
    asyncio.create_task(_discover_job(job_id))

    return {"result": "queued", "job_id": job_id, "model_name": model_name}


async def _tool_import_csv(args: dict) -> dict:
    csv_text = args["csv_text"]
    filter_year = args.get("filter_year")
    filter_month = args.get("filter_month", "").lower() if args.get("filter_month") else None
    filter_term = args.get("filter_term")
    max_items = args.get("max_items", 9999)

    reader = csv.DictReader(io.StringIO(csv_text))
    queued = 0
    skipped = 0
    errors = []

    for row in reader:
        if queued >= max_items:
            break

        year = row.get("Year", "").strip()
        month = row.get("Month", "").strip()
        term = row.get("Term", "").strip()
        model_name = row.get("Model Name", "").strip()
        product_url = row.get("Gumroad URL", "").strip()

        if not product_url or not model_name:
            continue

        if filter_year and str(filter_year) != year:
            continue
        if filter_month and filter_month != month.lower():
            continue
        if filter_term and filter_term.lower() != term.lower():
            continue

        if await db.job_exists(product_url):
            skipped += 1
            continue

        try:
            job_id = await db.create_job(model_name, term, product_url)
            import asyncio
            asyncio.create_task(_discover_job(job_id))
            queued += 1
        except Exception as e:
            errors.append(f"{model_name}: {e}")

    return {
        "queued": queued,
        "skipped_duplicates": skipped,
        "errors": errors[:10],
    }


async def _tool_list_files(args: dict) -> list:
    job_id = args["job_id"]
    files = await db.list_files(job_id)
    # Strip long CDN URLs from response to keep it readable
    return [
        {
            "id": f["id"],
            "filename": f["filename"],
            "status": f["status"],
            "size_mb": round(f["size_bytes"] / 1e6, 1) if f["size_bytes"] else 0,
            "error": f["error_msg"],
        }
        for f in files
    ]


async def _tool_list_library(args: dict) -> dict:
    category = args["category"]
    search = (args.get("search") or "").lower()
    path_map = settings.term_to_path
    base_path = path_map.get(category, "")

    if not os.path.isdir(base_path):
        return {"error": f"Path not found: {base_path}", "folders": []}

    folders = []
    for name in sorted(os.listdir(base_path)):
        if search and search not in name.lower():
            continue
        full = os.path.join(base_path, name)
        if os.path.isdir(full):
            zips = [f for f in os.listdir(full) if f.endswith(".zip")]
            folders.append({"name": name, "zip_count": len(zips)})

    return {"category": category, "path": base_path, "folder_count": len(folders), "folders": folders}


async def _tool_start_worker(_: dict) -> dict:
    if not worker._running:
        worker.start()
        return {"result": "started"}
    return {"result": "already running"}


async def _tool_stop_worker(_: dict) -> dict:
    worker.stop()
    return {"result": "stopped"}


async def _tool_plan_organize(args: dict) -> dict:
    """Dry-run: show what organize_folder would do without moving anything."""
    from app.folder_organizer import build_organization_plan
    category = args["category"]
    base_path = settings.term_to_path.get(category)
    if not base_path:
        return {"error": f"Unknown category: {category}"}
    plan = build_organization_plan(base_path)
    # Summarize (full move list can be very long)
    preview = plan["moves"][:30]
    return {
        "summary": plan["summary"],
        "preview_moves": [{"src": os.path.basename(m["src"]), "dest_folder": m["folder"]} for m in preview],
        "conflict_folders": plan["conflict_renames"],
        "note": f"Showing first {len(preview)} of {len(plan['moves'])} moves. Call organize_folder to execute.",
    }


async def _tool_organize_folder(args: dict) -> dict:
    """Execute folder organization for a category. Always does a dry_run first unless confirmed=True."""
    from app.folder_organizer import build_organization_plan, execute_organization_plan
    category = args["category"]
    confirmed = args.get("confirmed", False)
    base_path = settings.term_to_path.get(category)
    if not base_path:
        return {"error": f"Unknown category: {category}"}

    plan = build_organization_plan(base_path)

    if not confirmed:
        return {
            "status": "preview",
            "summary": plan["summary"],
            "message": f"Would move {plan['summary']['to_move']} files. Call again with confirmed=true to execute.",
        }

    result = execute_organization_plan(plan, dry_run=False)
    return {"status": "done", **result}


TOOL_HANDLERS = {
    "get_status": _tool_get_status,
    "list_jobs": _tool_list_jobs,
    "add_job": _tool_add_job,
    "import_csv": _tool_import_csv,
    "list_files": _tool_list_files,
    "list_library": _tool_list_library,
    "plan_organize": _tool_plan_organize,
    "organize_folder": _tool_organize_folder,
    "start_worker": _tool_start_worker,
    "stop_worker": _tool_stop_worker,
}

# ── Background: job discovery ─────────────────────────────────────────────────
# Limit to ONE concurrent Playwright/Chromium discovery at a time.
# Each discovery launches a headless browser; running many simultaneously
# would saturate the NAS CPU.  Jobs queue up here and run serially.
_discover_semaphore = asyncio.Semaphore(1)

# Maximum wall-clock time (seconds) allowed for a single discovery attempt.
_DISCOVER_TIMEOUT = 90


async def _discover_job(job_id: int):
    """
    Background task: navigate to the product's Gumroad page, extract
    all download file URLs, and insert them as 'pending' file rows.

    Serialised via _discover_semaphore — only one Playwright browser runs
    at a time regardless of how many jobs are queued.
    """
    # Fast pre-check before waiting for the semaphore: if the job was
    # already handled (reset, cancelled, ingest-ingested) skip it.
    job = await db.get_job(job_id)
    if not job or job["status"] not in ("pending", "discovering"):
        return

    async with _discover_semaphore:
        # Re-fetch after acquiring the lock — status may have changed while waiting
        job = await db.get_job(job_id)
        if not job or job["status"] not in ("pending", "discovering"):
            return

        await db.update_job(job_id, status="discovering")
        try:
            await asyncio.wait_for(_run_discovery(job_id, job), timeout=_DISCOVER_TIMEOUT)
        except asyncio.TimeoutError:
            logger.error(f"Discovery timed out for job {job_id} after {_DISCOVER_TIMEOUT}s")
            await db.update_job(job_id, status="error",
                                error_msg=f"Discovery timed out after {_DISCOVER_TIMEOUT}s")
        except Exception as e:
            logger.error(f"Discovery error for job {job_id}: {e}")
            await db.update_job(job_id, status="error", error_msg=str(e)[:500])


async def _run_discovery(job_id: int, job: dict):
    """Inner discovery logic, called under the semaphore with a timeout."""
    from app.gumroad import GumroadClient

    client = GumroadClient(settings.gumroad_cookies)
    product_url = job["product_url"]

    # Resolve to content URL if needed
    content_url = product_url
    if "/d/" not in product_url:
        resolved = await client.resolve_content_url(product_url)
        if not resolved:
            await db.update_job(job_id, status="error", error_msg="Could not find content URL")
            return
        content_url = resolved
        await db.update_job(job_id, content_url=content_url)

    # Get all downloadable files
    files = await client.get_download_files(content_url)

    if not files:
        await db.update_job(job_id, status="error", error_msg="No files found on content page")
        return

    # Determine destination base path
    term = job["term"]
    base_path = settings.term_to_path.get(term) or settings.archive_path
    from app.organizer import dest_path_for_file, clean_filename

    for f in files:
        filename = clean_filename(f["filename"])
        dest = dest_path_for_file(base_path, filename)
        await db.create_file(
            job_id=job_id,
            filename=filename,
            cdn_url=f["cdn_url"],
            dest_path=dest,
        )

    await db.update_job(job_id, status="queued", file_count=len(files), content_url=content_url)
    logger.info(f"Job {job_id} ({job['model_name']}): discovered {len(files)} files")


# ── Agent chat ────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are the WickedSync assistant — an intelligent download manager for a 3DWicked STL model collection.

You help the user:
- Queue Gumroad products for download (individually or from a CSV)
- Monitor download progress
- Browse and search the NAS library
- Control the download worker

The user's collection is organized across three NAS folders:
- Movies: movie-themed 3D models
- VG: video game-themed 3D models
- Marvel: Marvel Comics 3D models

Each product has multiple variant ZIP files (Non Supported, Pre Supported, Chitubox Pre Supported,
One Piece, X Pose, Images, etc.) — all variants are grouped in one subfolder per model name.

Be concise and action-oriented. Use tools proactively. When the user asks to download something,
call add_job or import_csv right away."""


class ClaudeAgent:
    def __init__(self):
        self._client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    async def chat(self, user_message: str, history: list[dict] | None = None) -> str:
        """
        Send a user message to Claude with tool support.
        history: list of {'role': 'user'|'assistant', 'content': '...'}
        Returns the assistant's final text reply.
        """
        messages = list(history or [])
        messages.append({"role": "user", "content": user_message})

        # Agentic loop: keep going until Claude stops using tools
        for _ in range(10):  # max 10 tool-use rounds
            response = await self._client.messages.create(
                model=settings.claude_model,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            )

            # Collect any text + tool use blocks
            assistant_content = response.content
            messages.append({"role": "assistant", "content": assistant_content})

            if response.stop_reason == "end_turn":
                # Extract text response
                text = " ".join(
                    block.text for block in assistant_content
                    if hasattr(block, "text")
                )
                return text.strip()

            if response.stop_reason == "tool_use":
                # Execute all tool calls, add results
                tool_results = []
                for block in assistant_content:
                    if block.type == "tool_use":
                        result = await self._call_tool(block.name, block.input)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(result, default=str),
                        })

                messages.append({"role": "user", "content": tool_results})
                continue

            break  # unexpected stop reason

        return "Sorry, I hit an unexpected state. Please try again."

    async def _call_tool(self, name: str, args: dict) -> Any:
        handler = TOOL_HANDLERS.get(name)
        if not handler:
            return {"error": f"Unknown tool: {name}"}
        try:
            return await handler(args)
        except Exception as e:
            logger.error(f"Tool {name} error: {e}")
            return {"error": str(e)}
