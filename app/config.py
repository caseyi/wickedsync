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
    concurrent_downloads: int = 6
    port: int = 8088
    movies_path: str = "/mnt/movies"
    vg_path: str = "/mnt/vg"
    marvel_path: str = "/mnt/marvel"
    db_path: str = "/app/data/wickedsync.db"
    claude_model: str = "claude-haiku-4-5-20251001"

    # Map CSV "Term" values to NAS paths
    @property
    def term_to_path(self) -> dict[str, str]:
        return {
            "Movies": self.movies_path,
            "VG": self.vg_path,
            "Marvel": self.marvel_path,
            "Wildcard": self.movies_path,  # default wildcard to movies
        }

    class Config:
        env_file = ".env"


settings = Settings()
