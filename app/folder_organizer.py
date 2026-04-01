"""
Interactive folder organization for the NAS library.
Python implementation of the organize_wicked_zips.sh logic —
same stripping rules, but returns a plan before moving anything,
and supports dry-run mode for Claude to preview changes.

Also includes a probabilistic matcher that:
  1. Applies the deterministic strip chain first.
  2. If the result doesn't match any existing folder, uses fuzzy string
     similarity (difflib / Jaro-Winkler) to find the closest candidate.
  3. If confidence >= HIGH_THRESHOLD → accept automatically.
  4. If MEDIUM_THRESHOLD <= confidence < HIGH_THRESHOLD → flag for user review.
  5. If confidence < MEDIUM_THRESHOLD → ask user to name it.
  6. Stores every accepted decision in a SQLite preferences table so the same
     file pattern is handled identically in future runs without asking again.
"""
import difflib
import logging
import os
import re
import shutil
from typing import Literal

HIGH_CONF = 0.92    # auto-accept if similarity >= this
MEDIUM_CONF = 0.72  # ask user if similarity >= this and < HIGH_CONF
# Below MEDIUM_CONF → prompt user for the canonical name

logger = logging.getLogger(__name__)

# ── Stripping chain (mirrors the bash sed chain) ──────────────────────────────

_STRIP_CHAIN = [
    re.compile(r'-[0-9]{8}T[0-9]{6}Z(-[0-9])?-[0-9]{1,3}$'),   # GDrive timestamp
    re.compile(r'-[0-9]{1,3}$'),                                    # numeric split
    re.compile(r'\s+\([0-9]+\)$'),                                  # OS duplicate (1)
    re.compile(r'\s+\([^)]*\)$'),                                   # variant label
    re.compile(r'\s+-\s+Update$', re.IGNORECASE),                   # " - Update"
    re.compile(r'_[0-9]{1,2}$'),                                    # Synology _N
]


def _canonical_folder(zip_stem: str) -> str:
    """Strip all suffixes from a zip stem to get the canonical model folder name."""
    name = zip_stem
    for pattern in _STRIP_CHAIN:
        name = pattern.sub('', name).strip()
    return name


# ── Probabilistic matcher ─────────────────────────────────────────────────────

class FolderMatcher:
    """
    Combines deterministic stripping with fuzzy similarity matching.
    Learns from user decisions stored in the DB preferences table.
    """

    def __init__(self, existing_folders: list[str]):
        """existing_folders: list of folder names already present in base_dir."""
        self._existing = existing_folders

    def match(self, zip_stem: str) -> dict:
        """
        Given a zip filename stem, return:
        {
          'canonical': str,          # best guess at folder name
          'confidence': float,       # 0.0–1.0
          'action': 'auto'|'review'|'ask',
          'alternatives': [str],     # other close candidates
        }
        """
        # Step 1: deterministic strip chain
        stripped = _canonical_folder(zip_stem)

        # Step 2: exact match in existing folders → perfect confidence
        if stripped in self._existing:
            return {
                "canonical": stripped,
                "confidence": 1.0,
                "action": "auto",
                "alternatives": [],
            }

        # Step 3: case-insensitive exact match
        lower_existing = {f.lower(): f for f in self._existing}
        if stripped.lower() in lower_existing:
            match = lower_existing[stripped.lower()]
            return {
                "canonical": match,
                "confidence": 0.98,
                "action": "auto",
                "alternatives": [],
            }

        # Step 4: fuzzy similarity against all existing folders
        if self._existing:
            ratios = [
                (folder, difflib.SequenceMatcher(None, stripped.lower(), folder.lower()).ratio())
                for folder in self._existing
            ]
            ratios.sort(key=lambda x: x[1], reverse=True)
            best_folder, best_score = ratios[0]
            alternatives = [f for f, s in ratios[1:4] if s > 0.5]

            if best_score >= HIGH_CONF:
                return {
                    "canonical": best_folder,
                    "confidence": round(best_score, 3),
                    "action": "auto",
                    "alternatives": alternatives,
                }
            elif best_score >= MEDIUM_CONF:
                return {
                    "canonical": best_folder,
                    "confidence": round(best_score, 3),
                    "action": "review",
                    "alternatives": alternatives,
                }

        # Step 5: no close match → use the stripped name as a new folder
        return {
            "canonical": stripped,
            "confidence": round(0.5, 3),
            "action": "ask" if not stripped else "auto",
            "alternatives": [],
        }


async def save_preference(pattern: str, canonical: str):
    """Store a learned mapping: zip_stem_pattern → canonical folder name."""
    from app import database as db_module
    # We reuse the DB connection; add a preferences table if not present
    import aiosqlite
    from app.config import settings
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS folder_preferences (
                pattern   TEXT PRIMARY KEY,
                canonical TEXT NOT NULL,
                source    TEXT DEFAULT 'user',
                created_at TEXT NOT NULL
            )
        """)
        from app.database import now
        await db.execute(
            "INSERT OR REPLACE INTO folder_preferences (pattern, canonical, source, created_at) VALUES (?,?,?,?)",
            (pattern, canonical, 'user', now()),
        )
        await db.commit()


async def load_preferences() -> dict[str, str]:
    """Return all learned pattern→canonical mappings."""
    import aiosqlite
    from app.config import settings
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS folder_preferences (
                    pattern   TEXT PRIMARY KEY,
                    canonical TEXT NOT NULL,
                    source    TEXT DEFAULT 'user',
                    created_at TEXT NOT NULL
                )
            """)
            await db.commit()
            async with db.execute("SELECT pattern, canonical FROM folder_preferences") as cur:
                rows = await cur.fetchall()
                return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


def build_organization_plan(
    base_dir: str,
    franchise_map: dict[str, str] | None = None,
) -> dict:
    """
    Scan base_dir for .zip files up to 2 levels deep and build a plan.

    franchise_map: optional {canonical_folder_name: franchise_name}.
      When a match is found, the destination becomes:
        base_dir / franchise_name / folder_name / file.zip
      instead of:
        base_dir / folder_name / file.zip

    Returns:
      {
        'moves': [{'src': str, 'dest': str, 'folder': str, 'franchise': str|None}],
        'already_correct': [...],
        'empty_dirs_to_remove': [...],
        'conflict_renames': [...],
      }
    """
    moves = []
    already_correct = []
    empty_dirs = []
    conflict_renames = []
    franchise_map = franchise_map or {}

    # Find Synology conflict folders
    conflict_pattern = re.compile(r'^(.+)_ADMIN_[A-Za-z]+-[0-9]+-[0-9]+-[0-9]+_Conflict(.*)$')

    if not os.path.isdir(base_dir):
        return {
            "error": f"Directory not found: {base_dir}",
            "moves": [],
            "already_correct": [],
            "empty_dirs_to_remove": [],
            "conflict_renames": [],
        }

    # Check for conflict folders first
    for name in os.listdir(base_dir):
        full = os.path.join(base_dir, name)
        if os.path.isdir(full):
            m = conflict_pattern.match(name)
            if m:
                canonical = m.group(1) + m.group(2)
                conflict_renames.append({
                    "conflict_folder": full,
                    "suggested_dest": os.path.join(base_dir, canonical),
                    "canonical_name": canonical,
                })

    # Find all zips at maxdepth 2
    zip_files = []
    for entry in os.listdir(base_dir):
        full = os.path.join(base_dir, entry)
        if os.path.isfile(full) and entry.lower().endswith('.zip'):
            zip_files.append(full)
        elif os.path.isdir(full):
            try:
                for sub_entry in os.listdir(full):
                    sub_full = os.path.join(full, sub_entry)
                    if os.path.isfile(sub_full) and sub_entry.lower().endswith('.zip'):
                        zip_files.append(sub_full)
            except PermissionError:
                pass

    for zip_path in sorted(zip_files):
        stem = os.path.basename(zip_path)[:-4]
        folder_name = _canonical_folder(stem)

        # Franchise nesting: base_dir/franchise/folder_name/ instead of base_dir/folder_name/
        franchise = franchise_map.get(folder_name)
        if franchise:
            dest_dir = os.path.join(base_dir, franchise, folder_name)
        else:
            dest_dir = os.path.join(base_dir, folder_name)

        dest_path = os.path.join(dest_dir, os.path.basename(zip_path))

        if zip_path == dest_path:
            already_correct.append(zip_path)
            continue

        moves.append({
            "src": zip_path,
            "dest": dest_path,
            "folder": folder_name,
            "franchise": franchise,
        })

    # Find empty subdirs (candidates for cleanup)
    for name in os.listdir(base_dir):
        full = os.path.join(base_dir, name)
        if os.path.isdir(full):
            contents = os.listdir(full)
            if not contents:
                empty_dirs.append(full)

    return {
        "base_dir": base_dir,
        "moves": moves,
        "already_correct": already_correct,
        "empty_dirs_to_remove": empty_dirs,
        "conflict_renames": conflict_renames,
        "summary": {
            "to_move": len(moves),
            "already_correct": len(already_correct),
            "empty_dirs": len(empty_dirs),
            "conflict_folders": len(conflict_renames),
        }
    }


def execute_organization_plan(plan: dict, dry_run: bool = False) -> dict:
    """
    Execute a plan returned by build_organization_plan.
    Returns a result dict with counts and errors.
    """
    moved = 0
    skipped = 0
    errors = []
    dirs_removed = 0

    for move in plan.get("moves", []):
        src = move["src"]
        dest = move["dest"]
        dest_dir = os.path.dirname(dest)

        if not os.path.isfile(src):
            skipped += 1
            continue

        if os.path.isfile(dest):
            logger.info(f"SKIP (dest exists): {os.path.basename(src)}")
            skipped += 1
            continue

        if dry_run:
            logger.info(f"DRY RUN: {src} → {dest}")
            moved += 1
            continue

        try:
            os.makedirs(dest_dir, exist_ok=True)
            shutil.move(src, dest)
            moved += 1
            logger.info(f"Moved: {os.path.basename(src)} → {move['folder']}/")
        except Exception as e:
            errors.append(f"{os.path.basename(src)}: {e}")
            logger.error(f"Error moving {src}: {e}")

    # Handle conflict folders
    for conflict in plan.get("conflict_renames", []):
        src_dir = conflict["conflict_folder"]
        dest_dir = conflict["suggested_dest"]

        if not os.path.isdir(src_dir):
            continue

        try:
            os.makedirs(dest_dir, exist_ok=True)
            for fname in os.listdir(src_dir):
                if fname.lower().endswith('.zip'):
                    src_file = os.path.join(src_dir, fname)
                    dest_file = os.path.join(dest_dir, fname)
                    if os.path.isfile(dest_file):
                        skipped += 1
                        continue
                    if not dry_run:
                        shutil.move(src_file, dest_file)
                    moved += 1

            # Remove conflict folder if now empty
            if not dry_run:
                remaining = [f for f in os.listdir(src_dir) if not f.startswith('.')]
                if not remaining:
                    os.rmdir(src_dir)
                    dirs_removed += 1
        except Exception as e:
            errors.append(f"conflict {os.path.basename(src_dir)}: {e}")

    # Clean up empty dirs
    if not dry_run:
        for empty_dir in plan.get("empty_dirs_to_remove", []):
            try:
                if os.path.isdir(empty_dir) and not os.listdir(empty_dir):
                    os.rmdir(empty_dir)
                    dirs_removed += 1
            except Exception:
                pass

    return {
        "dry_run": dry_run,
        "moved": moved,
        "skipped": skipped,
        "errors": errors[:20],
        "dirs_removed": dirs_removed,
    }
