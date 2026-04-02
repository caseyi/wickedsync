"""Configuration loaded from environment variables."""
import os
from pydantic_settings import BaseSettings

# Read codename from VERSION file (falls back gracefully if missing)
def _read_version() -> str:
    try:
        _here = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(_here, "VERSION")) as f:
            return f.read().strip()
    except Exception:
        return "unknown"

CODENAME = _read_version()


class Settings(BaseSettings):
    anthropic_api_key: str = ""
    gumroad_cookies: str = ""
    concurrent_downloads: int = 1
    port: int = 8088

    # Single archive root — all immediate subdirectories are selectable in the UI.
    # Defaults to /mnt/archive (mapped to /volume1/STL Archive in docker-compose).
    archive_path: str = "/mnt/archive"

    db_path: str = "/app/data/wickedsync.db"
    claude_model: str = "claude-haiku-4-5-20251001"

    # ── Legacy per-category paths (kept for backwards-compat during transition) ──
    # If archive_path is mounted these are ignored; only used when archive_path
    # doesn't exist (e.g. local dev without a volume).
    movies_path: str = "/mnt/movies"
    vg_path: str = "/mnt/vg"
    marvel_path: str = "/mnt/marvel"

    @property
    def archive_subdirs(self) -> dict[str, str]:
        """
        Return a dict of {display_name: absolute_path} for every immediate
        subdirectory of archive_path that actually exists.

        Also includes a synthetic "(All)" entry pointing at archive_path itself
        so callers can iterate the whole archive as one flat namespace.

        Falls back to the legacy individual paths if archive_path isn't mounted.
        """
        if os.path.isdir(self.archive_path):
            dirs: dict[str, str] = {}
            try:
                for name in sorted(os.listdir(self.archive_path)):
                    full = os.path.join(self.archive_path, name)
                    if os.path.isdir(full) and not name.startswith("."):
                        dirs[name] = full
            except PermissionError:
                pass
            return dirs
        # Fallback to legacy mounts
        legacy = {
            "Movies": self.movies_path,
            "Video Games": self.vg_path,
            "Marvel": self.marvel_path,
        }
        return {k: v for k, v in legacy.items() if os.path.isdir(v)}

    @property
    def term_to_path(self) -> dict[str, str]:
        """
        Legacy property — maps Gumroad CSV "Term" values to NAS paths.
        Used by download routing (CSV import, job creation).

        Derives paths from archive_subdirs where possible; falls back to
        old individual mounts so existing jobs continue to work.
        """
        subdirs = self.archive_subdirs
        result: dict[str, str] = {}

        # Try to map well-known term names to subdir names
        term_aliases = {
            "Movies": ["Movies", "Movie", "movies"],
            "VG": ["Video Games", "Games", "VG", "vg"],
            "Marvel": ["Marvel", "marvel"],
            "Wildcard": [],  # handled below
        }
        for term, aliases in term_aliases.items():
            for alias in aliases:
                if alias in subdirs:
                    result[term] = subdirs[alias]
                    break
            else:
                # Not found in subdirs — use legacy path
                if term == "Movies" and os.path.isdir(self.movies_path):
                    result[term] = self.movies_path
                elif term == "VG" and os.path.isdir(self.vg_path):
                    result[term] = self.vg_path
                elif term == "Marvel" and os.path.isdir(self.marvel_path):
                    result[term] = self.marvel_path

        # Wildcard defaults to Movies if available, else first available path
        if "Movies" in result:
            result["Wildcard"] = result["Movies"]
        elif result:
            result["Wildcard"] = next(iter(result.values()))

        return result

    def resolve_working_path(self, requested: str | None) -> str | None:
        """
        Validate and return a safe absolute path for library operations.

        `requested` may be:
          - None / ""           → returns archive_path (whole archive)
          - a subdir name       → e.g. "Movies"  (looked up in archive_subdirs)
          - an absolute path    → must be under archive_path (safety check)

        Returns None if the path is invalid / not accessible.
        """
        archive = self.archive_path

        if not requested:
            return archive if os.path.isdir(archive) else None

        # Subdir shorthand (e.g. "Movies")
        subdirs = self.archive_subdirs
        if requested in subdirs:
            return subdirs[requested]

        # Absolute path — must be under archive_path
        if os.path.isabs(requested):
            real = os.path.realpath(requested)
            real_archive = os.path.realpath(archive)
            if real.startswith(real_archive + os.sep) or real == real_archive:
                return real if os.path.isdir(real) else None

        return None

    class Config:
        env_file = ".env"


settings = Settings()
