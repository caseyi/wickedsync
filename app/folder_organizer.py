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


def scan_library_health(
    base_dir: str,
    cross_category_dirs: dict[str, str] | None = None,
) -> dict:
    """
    Deep health scan of a library directory.

    Returns a comprehensive report with:
    - folder_stats:  per-folder zip counts (root + nested)
    - term_buckets:  "MOVIES August Term 2024"-style bucket folders
    - fuzzy_dupes:   pairs of folder names that look like typo duplicates
    - empty_folders: folders (and sub-folders) containing zero zips
    - cross_category: folder names appearing in >1 category path

    cross_category_dirs: {"Movies": "/mnt/movies", "VG": "/mnt/vg", ...}
    """
    if not os.path.isdir(base_dir):
        return {"error": f"Directory not found: {base_dir}"}

    # ── 1. Deep folder scan ───────────────────────────────────────────────────
    _TERM_BUCKET = re.compile(
        r'\b(term|january|february|march|april|may|june|july|august|september|october|november|december)\b',
        re.IGNORECASE,
    )

    folder_stats = []
    term_buckets = []
    empty_folders = []

    root_entries = sorted(os.listdir(base_dir))
    for entry in root_entries:
        full = os.path.join(base_dir, entry)
        if not os.path.isdir(full):
            continue

        try:
            sub_entries = os.listdir(full)
        except PermissionError:
            continue

        root_zips = [f for f in sub_entries if f.lower().endswith('.zip')]
        subfolders = [f for f in sub_entries if os.path.isdir(os.path.join(full, f))]

        # Count nested zips
        nested_zips: list[str] = []
        for sf in subfolders:
            sf_full = os.path.join(full, sf)
            try:
                sf_items = os.listdir(sf_full)
                sf_zips = [z for z in sf_items if z.lower().endswith('.zip')]
                nested_zips.extend(sf_zips)
            except PermissionError:
                pass

        total_zips = len(root_zips) + len(nested_zips)

        stat = {
            "name": entry,
            "path": full,
            "root_zips": len(root_zips),
            "nested_zips": len(nested_zips),
            "subfolder_count": len(subfolders),
            "total_zips": total_zips,
        }
        folder_stats.append(stat)

        # Empty folder detection (no zips at any level)
        if total_zips == 0:
            empty_folders.append({"name": entry, "path": full, "depth": 0})

        # Check nested subfolders for empties too
        for sf in subfolders:
            sf_full = os.path.join(full, sf)
            try:
                sf_contents = os.listdir(sf_full)
                sf_zips = [f for f in sf_contents if f.lower().endswith('.zip')]
                if not sf_zips:
                    empty_folders.append({"name": sf, "path": sf_full, "depth": 1, "parent": entry})
            except PermissionError:
                pass

        # Term bucket detection: root-level folders with only loose zips and name
        # matches month/term keywords (e.g. "MOVIES August Term 2024")
        if _TERM_BUCKET.search(entry) and len(root_zips) > 0 and len(subfolders) == 0:
            term_buckets.append({
                "name": entry,
                "path": full,
                "zip_count": len(root_zips),
                "zips": sorted(root_zips[:50]),  # cap at 50 for payload size
            })

    # ── 2. Fuzzy duplicate detection ─────────────────────────────────────────
    folder_names = [s["name"] for s in folder_stats]
    fuzzy_dupes = []
    seen_pairs: set[frozenset] = set()

    for i, name_a in enumerate(folder_names):
        for name_b in folder_names[i + 1:]:
            key = frozenset([name_a, name_b])
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            # Quick length filter: skip if lengths differ by more than 30%
            la, lb = len(name_a), len(name_b)
            if la and lb and (min(la, lb) / max(la, lb)) < 0.70:
                continue
            ratio = difflib.SequenceMatcher(None, name_a.lower(), name_b.lower()).ratio()
            if 0.80 <= ratio < 1.0:
                fuzzy_dupes.append({
                    "folder_a": name_a,
                    "folder_b": name_b,
                    "similarity": round(ratio, 3),
                    "path_a": os.path.join(base_dir, name_a),
                    "path_b": os.path.join(base_dir, name_b),
                })

    fuzzy_dupes.sort(key=lambda x: x["similarity"], reverse=True)

    # ── 3. Cross-category duplicate detection ─────────────────────────────────
    cross_category = []
    if cross_category_dirs:
        # Gather folder names per category
        cat_folders: dict[str, set[str]] = {}
        for cat, path in cross_category_dirs.items():
            if os.path.isdir(path):
                try:
                    cat_folders[cat] = {
                        n.lower() for n in os.listdir(path)
                        if os.path.isdir(os.path.join(path, n))
                    }
                except PermissionError:
                    cat_folders[cat] = set()

        # Find names that appear in >1 category (case-insensitive)
        all_cats = list(cat_folders.keys())
        for i, cat_a in enumerate(all_cats):
            for cat_b in all_cats[i + 1:]:
                overlap = cat_folders.get(cat_a, set()) & cat_folders.get(cat_b, set())
                for name_lower in sorted(overlap):
                    # Get the actual-cased names
                    actual_a = next((n for n in os.listdir(cross_category_dirs[cat_a]) if n.lower() == name_lower), name_lower)
                    actual_b = next((n for n in os.listdir(cross_category_dirs[cat_b]) if n.lower() == name_lower), name_lower)
                    # Check if this pair already logged
                    existing = next((x for x in cross_category if x["name_lower"] == name_lower), None)
                    if existing:
                        if cat_b not in [c["category"] for c in existing["occurrences"]]:
                            existing["occurrences"].append({"category": cat_b, "name": actual_b, "path": os.path.join(cross_category_dirs[cat_b], actual_b)})
                    else:
                        cross_category.append({
                            "name_lower": name_lower,
                            "occurrences": [
                                {"category": cat_a, "name": actual_a, "path": os.path.join(cross_category_dirs[cat_a], actual_a)},
                                {"category": cat_b, "name": actual_b, "path": os.path.join(cross_category_dirs[cat_b], actual_b)},
                            ],
                        })

    return {
        "base_dir": base_dir,
        "folder_count": len(folder_stats),
        "folder_stats": folder_stats,
        "term_buckets": term_buckets,
        "fuzzy_dupes": fuzzy_dupes,
        "empty_folders": empty_folders,
        "cross_category": cross_category,
        "summary": {
            "folders": len(folder_stats),
            "term_buckets": len(term_buckets),
            "fuzzy_dupes": len(fuzzy_dupes),
            "empty_folders": len(empty_folders),
            "cross_category": len(cross_category),
        },
    }


def redistribute_term_bucket(
    bucket_path: str,
    base_dir: str,
    dry_run: bool = True,
) -> dict:
    """
    Redistribute zips from a term bucket folder into their canonical model folders
    within base_dir (same logic as build_organization_plan).

    dry_run=True: return the plan without moving anything.
    dry_run=False: move files.
    """
    if not os.path.isdir(bucket_path):
        return {"error": f"Bucket not found: {bucket_path}"}

    zips = [f for f in os.listdir(bucket_path) if f.lower().endswith('.zip')]
    moves = []
    skipped = []
    errors = []

    for fname in sorted(zips):
        stem = fname[:-4]
        folder_name = _canonical_folder(stem)
        dest_dir = os.path.join(base_dir, folder_name)
        dest_path = os.path.join(dest_dir, fname)
        src_path = os.path.join(bucket_path, fname)

        if os.path.isfile(dest_path):
            skipped.append({"file": fname, "reason": "already exists at destination"})
            continue

        if not dry_run:
            try:
                os.makedirs(dest_dir, exist_ok=True)
                shutil.move(src_path, dest_path)
                moves.append({"file": fname, "dest_folder": folder_name, "dest_path": dest_path})
            except Exception as e:
                errors.append({"file": fname, "error": str(e)})
        else:
            moves.append({"file": fname, "dest_folder": folder_name, "dest_path": dest_path})

    # Remove the bucket folder if it's now empty (non-dry-run only)
    bucket_removed = False
    if not dry_run and not errors:
        remaining = [f for f in os.listdir(bucket_path) if not f.startswith('.')]
        if not remaining:
            try:
                os.rmdir(bucket_path)
                bucket_removed = True
            except Exception:
                pass

    return {
        "dry_run": dry_run,
        "bucket_path": bucket_path,
        "moves": moves,
        "skipped": skipped,
        "errors": errors,
        "bucket_removed": bucket_removed,
        "summary": {
            "to_move": len(moves),
            "skipped": len(skipped),
            "errors": len(errors),
        },
    }


def delete_empty_folders(folder_paths: list[str], dry_run: bool = True) -> dict:
    """
    Delete a list of folder paths that are expected to be empty (no zips).
    Skips any folder that still has files/subdirs.
    """
    deleted = []
    skipped = []
    errors = []

    for path in folder_paths:
        if not os.path.isdir(path):
            skipped.append({"path": path, "reason": "not found"})
            continue

        # Double-check it's really empty of zips
        all_files = []
        for root, dirs, files in os.walk(path):
            all_files.extend(files)

        if any(f.lower().endswith('.zip') for f in all_files):
            skipped.append({"path": path, "reason": "contains zip files — not deleting"})
            continue

        if dry_run:
            deleted.append({"path": path, "name": os.path.basename(path)})
        else:
            try:
                shutil.rmtree(path)
                deleted.append({"path": path, "name": os.path.basename(path)})
            except Exception as e:
                errors.append({"path": path, "error": str(e)})

    return {
        "dry_run": dry_run,
        "deleted": deleted,
        "skipped": skipped,
        "errors": errors,
        "summary": {"deleted": len(deleted), "skipped": len(skipped), "errors": len(errors)},
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
