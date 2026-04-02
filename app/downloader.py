"""
Async download manager with configurable concurrency.
Downloads directly from CDN URLs (CloudFront / files.gumroad.com).
Works because the Docker container uses host networking → same public IP as NAS.

Includes randomised timing (jitter, staggered starts, UA rotation) to behave
more like an organic browser session and reduce rate-limit exposure.
"""
import asyncio
import logging
import os
import random
from typing import Callable, Optional

import aiohttp
import aiofiles

from app.config import settings
from app.organizer import ensure_dest_dir

logger = logging.getLogger(__name__)

# ── Realistic User-Agent pool ─────────────────────────────────────────────────
# Rotated per-download so requests don't all share an identical fingerprint.

_USER_AGENTS = [
    # Chrome on macOS (various versions)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]

# Chunk size range (bytes) — randomised per file so bandwidth pattern varies
_CHUNK_MIN = 128 * 1024   # 128 KB
_CHUNK_MAX = 512 * 1024   # 512 KB

# Inter-download jitter range (seconds) — pause between each download start
_JITTER_MIN = 1.5
_JITTER_MAX = 6.0

# Stagger delay applied sequentially when launching a batch (seconds per slot)
_STAGGER_MIN = 0.8
_STAGGER_MAX = 2.5

# Idle poll interval range when the queue is empty
_IDLE_MIN = 8
_IDLE_MAX = 20


def _pick_ua() -> str:
    return random.choice(_USER_AGENTS)


def _jitter() -> float:
    return random.uniform(_JITTER_MIN, _JITTER_MAX)


def _chunk_size() -> int:
    return random.randint(_CHUNK_MIN, _CHUNK_MAX)


# Semaphore is created once and shared across all download tasks
_semaphore: Optional[asyncio.Semaphore] = None


def get_semaphore() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(settings.concurrent_downloads)
    return _semaphore


async def download_file(
    cdn_url: str,
    dest_path: str,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> int:
    """
    Download a single file from cdn_url to dest_path.
    Uses the shared concurrency semaphore.
    Returns number of bytes downloaded.
    Raises on HTTP error or write error.
    """
    ensure_dest_dir(dest_path)

    async with get_semaphore():
        chunk_size = _chunk_size()
        headers = {
            "User-Agent": _pick_ua(),
            "Referer": "https://gumroad.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
        }

        connector = aiohttp.TCPConnector(ssl=True)
        timeout = aiohttp.ClientTimeout(total=3600, connect=30)  # 1hr max

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(cdn_url, headers=headers, allow_redirects=True) as resp:
                if resp.status != 200:
                    raise RuntimeError(
                        f"HTTP {resp.status} downloading {cdn_url}"
                    )

                total = int(resp.headers.get("Content-Length", 0))
                downloaded = 0

                # Write to a temp file first, rename on success (atomic-ish)
                tmp_path = dest_path + ".tmp"
                async with aiofiles.open(tmp_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        if on_progress:
                            on_progress(downloaded, total)

                os.rename(tmp_path, dest_path)
                logger.info(
                    f"Downloaded {os.path.basename(dest_path)} "
                    f"({downloaded:,} bytes, chunk={chunk_size//1024}KB)"
                )
                return downloaded


async def download_file_with_retry(
    cdn_url: str,
    dest_path: str,
    retries: int = 3,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> int:
    """Download with automatic retry on transient errors, with randomised backoff."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return await download_file(cdn_url, dest_path, on_progress)
        except Exception as e:
            last_error = e
            if attempt < retries:
                # Randomised exponential backoff: base 2^attempt ± 50% jitter
                base = 2 ** attempt
                wait = base + random.uniform(-base * 0.5, base * 0.5)
                wait = max(1.0, wait)
                logger.warning(
                    f"Attempt {attempt} failed for {os.path.basename(dest_path)}: {e}. "
                    f"Retrying in {wait:.1f}s…"
                )
                await asyncio.sleep(wait)
            else:
                logger.error(f"All {retries} attempts failed for {os.path.basename(dest_path)}: {e}")

    raise last_error


# ── Worker loop ───────────────────────────────────────────────────────────────

class DownloadWorker:
    """
    Background worker that polls the DB for pending files and downloads them.
    Started once at app startup; runs as an asyncio task.

    Timing strategy:
    - Staggered batch start: concurrent downloads are launched with a small
      random offset between each, not all at once.
    - Per-download jitter: each _download_one sleeps a random amount before
      beginning the actual HTTP request.
    - Idle randomisation: when the queue is empty the poll interval is random
      so the polling pattern is not a fixed heartbeat.
    """

    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            logger.info("Download worker started")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    async def _run(self):
        from app import database as db  # avoid circular import

        while self._running:
            try:
                pending = await db.get_pending_files()
                if not pending:
                    idle_wait = random.uniform(_IDLE_MIN, _IDLE_MAX)
                    await asyncio.sleep(idle_wait)
                    continue

                # Stagger task creation: each download slot starts after a
                # small random offset so concurrent requests don't all fire
                # simultaneously.
                tasks = []
                stagger_offset = 0.0
                for i, file_row in enumerate(pending):
                    if file_row["cdn_url"]:
                        stagger_offset += random.uniform(_STAGGER_MIN, _STAGGER_MAX)
                        tasks.append(
                            self._download_one(file_row, pre_delay=stagger_offset if i > 0 else 0.0)
                        )

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    # Brief pause between batches
                    await asyncio.sleep(random.uniform(2.0, 5.0))
                else:
                    await asyncio.sleep(random.uniform(_IDLE_MIN, _IDLE_MAX))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(5)

    async def _download_one(self, file_row: dict, pre_delay: float = 0.0):
        from app import database as db

        file_id = file_row["id"]

        # Stagger delay before acquiring semaphore
        if pre_delay > 0:
            await asyncio.sleep(pre_delay)

        # Per-download jitter (independent of stagger)
        jitter = _jitter()
        logger.debug(f"Jitter {jitter:.1f}s before {os.path.basename(file_row['dest_path'])}")
        await asyncio.sleep(jitter)

        await db.update_file(file_id, status="downloading")

        try:
            size = await download_file_with_retry(
                cdn_url=file_row["cdn_url"],
                dest_path=file_row["dest_path"],
            )
            await db.update_file(file_id, status="done", size_bytes=size)

            # Update parent job's done_count
            job_id = file_row["job_id"]
            job = await db.get_job(job_id)
            if job:
                done = job["done_count"] + 1
                new_status = "done" if done >= job["file_count"] else "downloading"
                await db.update_job(job_id, done_count=done, status=new_status)

        except Exception as e:
            await db.update_file(file_id, status="error", error_msg=str(e)[:500])
            logger.error(f"Failed to download file {file_id}: {e}")


# Singleton worker
worker = DownloadWorker()
