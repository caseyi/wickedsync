"""
File naming and folder organization logic.
Mirrors the logic in organize_wicked_zips.sh but in Python,
so the app can decide destination paths before downloading.
"""
import re
import os


# Variant labels that should NOT appear in folder names
_VARIANT_RE = re.compile(
    r'\s*\((Non Supported|Chitubox Pre Supported|Pre Supported|Stl Pre Supported|'
    r'One Piece|X Pose|Images|Size|Supported|FDM|Resin|Update)\)\s*$',
    re.IGNORECASE,
)

# Patterns to strip from filenames to get the canonical model name
_STRIP_PATTERNS = [
    re.compile(r'-[0-9]{8}T[0-9]{6}Z(-[0-9])?-[0-9]{1,3}$'),  # GDrive timestamp split
    re.compile(r'-[0-9]{1,3}$'),                                  # numeric split suffix
    re.compile(r'\s+\([0-9]+\)$'),                                # OS duplicate (1) (2)
    re.compile(r'\s+\([^)]*\)$'),                                 # variant label in parens
    re.compile(r'\s+-\s+Update$', re.IGNORECASE),                 # " - Update"
    re.compile(r'_[0-9]{1,2}$'),                                  # Synology _2 _3 suffix
]


def derive_folder_name(filename: str) -> str:
    """
    Given a zip filename (with or without .zip extension),
    return the canonical folder name to group all variants together.

    e.g. "Wicked - Blade (Non Supported).zip" → "Wicked - Blade"
         "Wicked - Blade (Chitubox Pre Supported) (1).zip" → "Wicked - Blade"
    """
    name = filename
    # Strip .zip extension
    if name.lower().endswith('.zip'):
        name = name[:-4]

    for pattern in _STRIP_PATTERNS:
        name = pattern.sub('', name).strip()

    return name.strip()


def dest_path_for_file(base_dir: str, filename: str) -> str:
    """
    Build the full destination path for a file.
    Creates: base_dir/FolderName/filename
    """
    folder = derive_folder_name(filename)
    return os.path.join(base_dir, folder, filename)


def ensure_dest_dir(dest_path: str):
    """Create parent directories if they don't exist."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)


def clean_filename(raw_name: str) -> str:
    """
    Sanitize a filename coming from Gumroad (URL-decoded).
    Removes path traversal, strips leading/trailing whitespace.
    """
    # Remove any directory separators
    name = os.path.basename(raw_name)
    # Remove null bytes
    name = name.replace('\x00', '')
    return name.strip()
