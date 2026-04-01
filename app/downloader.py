"""
Async download manager with configurable concurrency.
Downloads directly from CDN URLs (CloudFront / files.gumroad.com).
Works because the Docker container uses host networking → same public IP as NAS.
"""
import asyncio
import logging
import os
from typing import Callable, Optional

import aiohttp
import aiofiles

from app.config import settings
from app.organizer import ensure_dest_dir

logger = logging.getLogger(__name__)

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
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Referer": "https://gumroad.com/",
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
                    async for chunk in resp.content.iter_chunked(1024 * 256):  # 256KB chunks
                        await f.write(chunk)
                        downloaded += len(chunk)
                        if on_progress:
                            on_progress(downloaded, total)

                os.rename(tmp_path, dest_path)
                logger.info(f"Downloaded {os.path.basename(dest_path)} ({downloaded:,} bytes)")
                return downloaded


async def download_file_with_retry(
    cdn_url: str,
    dest_path: str,
    retries: int = 3,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> int:
    """Download with automatic retry on transient errors."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return await download_file(cdn_url, dest_path, on_progress)
        except Exception as e:
            last_error = e
            if attempt < retries:
                wait = 2 ** attempt  # exponential backoff: 2, 4, 8 seconds
                logger.warning(f"Attempt {attempt} failed for {os.path.basename(dest_path)}: {e}. Retrying in {wait}s…")
                await asyncio.sleep(wait)
            else:
                logger.error(f"All {retries} attempts failed for {os.path.basename(dest_path)}: {e}")

    raise last_error


# ── Worker loop ───────────────────────────────────────────────────────────────

class DownloadWorker:
    """
    Background worker that polls the DB for pending files and downloads them.
    Started once at app startup; runs as an asyncio task.
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
                    await asyncio.sleep(5)
                    continue

                tasks = []
                for file_row in pending:
                    if file_row["cdn_url"]:
                        tasks.append(self._download_one(file_row))

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                else:
                    await asyncio.sleep(5)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(5)

    async def _download_one(self, file_row: dict):
        from app import database as db

        file_id = file_row["id"]
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
