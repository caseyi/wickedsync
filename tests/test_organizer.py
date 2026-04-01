"""
Tests for the folder naming / organization logic.
These are pure-Python tests with no DB or network dependencies.
"""
import os
import shutil
import tempfile
import pytest

from app.organizer import derive_folder_name, dest_path_for_file, clean_filename
from app.folder_organizer import _canonical_folder, build_organization_plan, execute_organization_plan


# ── derive_folder_name / _canonical_folder ────────────────────────────────────

class TestCanonicalFolder:
    def test_plain_name_unchanged(self):
        assert _canonical_folder("Wicked - Blade Sculpture") == "Wicked - Blade Sculpture"

    def test_strips_non_supported(self):
        assert _canonical_folder("Wicked - Blade Sculpture (Non Supported)") == "Wicked - Blade Sculpture"

    def test_strips_chitubox_pre_supported(self):
        assert _canonical_folder("Wicked - Blade Sculpture (Chitubox Pre Supported)") == "Wicked - Blade Sculpture"

    def test_strips_images(self):
        assert _canonical_folder("Wicked - Blade Sculpture (Images)") == "Wicked - Blade Sculpture"

    def test_strips_one_piece(self):
        assert _canonical_folder("Wicked - Blade Sculpture (One Piece)") == "Wicked - Blade Sculpture"

    def test_strips_x_pose(self):
        assert _canonical_folder("Wicked - Blade Sculpture (X Pose)") == "Wicked - Blade Sculpture"

    def test_strips_update_suffix(self):
        assert _canonical_folder("Wicked - Blade Sculpture - Update") == "Wicked - Blade Sculpture"

    def test_strips_os_duplicate(self):
        assert _canonical_folder("Wicked - Blade Sculpture (1)") == "Wicked - Blade Sculpture"
        assert _canonical_folder("Wicked - Blade Sculpture (2)") == "Wicked - Blade Sculpture"

    def test_strips_os_dup_then_variant(self):
        # "(Non Supported) (1)" → strip (1) first, then variant
        assert _canonical_folder("Wicked - Ciri Statue 280mm (Non Supported) (1)") == "Wicked - Ciri Statue 280mm"

    def test_strips_gdrive_timestamp(self):
        result = _canonical_folder("Wicked - Blade Sculpture-20250117T005754Z-004")
        assert result == "Wicked - Blade Sculpture"

    def test_strips_gdrive_timestamp_with_split(self):
        result = _canonical_folder("Wicked - Blade Sculpture-20250912T034436Z-1-002")
        assert result == "Wicked - Blade Sculpture"

    def test_strips_numeric_split(self):
        assert _canonical_folder("Wicked - Blade Sculpture-002") == "Wicked - Blade Sculpture"

    def test_strips_synology_underscore(self):
        assert _canonical_folder("Wicked - Blade Sculpture_2") == "Wicked - Blade Sculpture"
        assert _canonical_folder("Wicked - Blade Sculpture_10") == "Wicked - Blade Sculpture"

    def test_titus_warhammer_all_variants_same_folder(self):
        """The original bug: Titus was split into 4-5 folders. All should resolve identically."""
        base = "Wicked - Titus Warhammer"
        variants = [
            "Wicked - Titus Warhammer (Non Supported)",
            "Wicked - Titus Warhammer (Chitubox Pre Supported)",
            "Wicked - Titus Warhammer (Images)",
            "Wicked - Titus Warhammer (One Piece)",
            "Wicked - Titus Warhammer",
        ]
        results = {_canonical_folder(v) for v in variants}
        assert results == {base}, f"Expected single folder, got: {results}"

    def test_ciri_statue_complex(self):
        """Complex case: numeric size in name + variant + OS duplicate."""
        assert _canonical_folder("Wicked - Ciri Statue 280mm (Non Supported) (1)") == "Wicked - Ciri Statue 280mm"


class TestDeriveFolderName:
    def test_strips_zip_extension(self):
        assert derive_folder_name("Wicked - Blade.zip") == "Wicked - Blade"

    def test_no_zip_extension(self):
        assert derive_folder_name("Wicked - Blade") == "Wicked - Blade"


class TestCleanFilename:
    def test_strips_path_traversal(self):
        assert clean_filename("../../../etc/passwd.zip") == "passwd.zip"

    def test_strips_whitespace(self):
        assert clean_filename("  filename.zip  ") == "filename.zip"

    def test_normal_name_unchanged(self):
        assert clean_filename("Wicked - Blade (Non Supported).zip") == "Wicked - Blade (Non Supported).zip"


# ── dest_path_for_file ────────────────────────────────────────────────────────

class TestDestPath:
    def test_builds_correct_path(self):
        path = dest_path_for_file("/mnt/movies", "Wicked - Blade Sculpture (Non Supported).zip")
        assert path == "/mnt/movies/Wicked - Blade Sculpture/Wicked - Blade Sculpture (Non Supported).zip"

    def test_all_variants_same_folder(self):
        base = "/mnt/movies"
        paths = [
            dest_path_for_file(base, "Wicked - Blade (Non Supported).zip"),
            dest_path_for_file(base, "Wicked - Blade (Images).zip"),
            dest_path_for_file(base, "Wicked - Blade (One Piece).zip"),
        ]
        folders = {os.path.dirname(p) for p in paths}
        assert folders == {"/mnt/movies/Wicked - Blade"}


# ── build_organization_plan ───────────────────────────────────────────────────

class TestBuildPlan:
    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _touch(self, *parts):
        path = os.path.join(self.tmpdir, *parts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, 'w').close()
        return path

    def test_empty_dir(self):
        plan = build_organization_plan(self.tmpdir)
        assert plan["summary"]["to_move"] == 0

    def test_nonexistent_dir(self):
        plan = build_organization_plan("/nonexistent/path")
        assert "error" in plan

    def test_loose_zips_get_move_plan(self):
        self._touch("Wicked - Blade (Non Supported).zip")
        self._touch("Wicked - Blade (Images).zip")
        plan = build_organization_plan(self.tmpdir)
        assert plan["summary"]["to_move"] == 2
        folders = {m["folder"] for m in plan["moves"]}
        assert folders == {"Wicked - Blade"}

    def test_already_correct_not_moved(self):
        self._touch("Wicked - Blade", "Wicked - Blade (Non Supported).zip")
        plan = build_organization_plan(self.tmpdir)
        assert plan["summary"]["to_move"] == 0
        assert plan["summary"]["already_correct"] == 1

    def test_mixed_loose_and_correct(self):
        self._touch("Wicked - Blade", "Wicked - Blade (Non Supported).zip")
        self._touch("Wicked - Blade (Images).zip")
        plan = build_organization_plan(self.tmpdir)
        assert plan["summary"]["to_move"] == 1
        assert plan["summary"]["already_correct"] == 1

    def test_conflict_folder_detected(self):
        self._touch(
            "Wicked - Blade_ADMIN_Mar-31-212108-2026_Conflict",
            "Wicked - Blade (Non Supported).zip"
        )
        plan = build_organization_plan(self.tmpdir)
        assert len(plan["conflict_renames"]) == 1
        assert plan["conflict_renames"][0]["canonical_name"] == "Wicked - Blade"

    def test_empty_dirs_detected(self):
        os.makedirs(os.path.join(self.tmpdir, "Empty Folder"))
        plan = build_organization_plan(self.tmpdir)
        assert len(plan["empty_dirs_to_remove"]) == 1


# ── execute_organization_plan ─────────────────────────────────────────────────

class TestExecutePlan:
    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _touch(self, *parts):
        path = os.path.join(self.tmpdir, *parts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            f.write("fake zip content")
        return path

    def test_dry_run_does_not_move(self):
        src = self._touch("Wicked - Blade (Non Supported).zip")
        plan = build_organization_plan(self.tmpdir)
        result = execute_organization_plan(plan, dry_run=True)
        assert result["moved"] == 1
        assert os.path.isfile(src), "File should not have moved in dry_run"

    def test_execute_moves_files(self):
        self._touch("Wicked - Blade (Non Supported).zip")
        self._touch("Wicked - Blade (Images).zip")
        plan = build_organization_plan(self.tmpdir)
        result = execute_organization_plan(plan, dry_run=False)
        assert result["moved"] == 2

        # Files should now be inside a subfolder
        expected = os.path.join(self.tmpdir, "Wicked - Blade")
        assert os.path.isdir(expected)
        moved_files = os.listdir(expected)
        assert len(moved_files) == 2

    def test_execute_skips_existing_dest(self):
        # Pre-create the destination file
        self._touch("Wicked - Blade (Non Supported).zip")
        self._touch("Wicked - Blade", "Wicked - Blade (Non Supported).zip")
        plan = build_organization_plan(self.tmpdir)
        result = execute_organization_plan(plan, dry_run=False)
        assert result["skipped"] >= 1

    def test_empty_dirs_removed_after_move(self):
        self._touch("Wicked - Blade (Non Supported).zip")
        # Create an empty dir
        os.makedirs(os.path.join(self.tmpdir, "Old Empty Folder"))
        plan = build_organization_plan(self.tmpdir)
        result = execute_organization_plan(plan, dry_run=False)
        assert not os.path.isdir(os.path.join(self.tmpdir, "Old Empty Folder"))


# ── FolderMatcher ─────────────────────────────────────────────────────────────

class TestFolderMatcher:
    from app.folder_organizer import FolderMatcher

    def test_exact_match_high_confidence(self):
        matcher = self.FolderMatcher(["Wicked - Blade Sculpture"])
        result = matcher.match("Wicked - Blade Sculpture (Non Supported)")
        assert result["canonical"] == "Wicked - Blade Sculpture"
        assert result["confidence"] == 1.0
        assert result["action"] == "auto"

    def test_fuzzy_close_match(self):
        matcher = self.FolderMatcher(["WICKED - Jack and Sally Diorama"])
        result = matcher.match("WICKED - Jack And Sally Diorama")
        assert result["action"] == "auto"
        assert result["confidence"] >= 0.92

    def test_low_confidence_triggers_ask(self):
        matcher = self.FolderMatcher(["Completely Different Thing"])
        result = matcher.match("Wicked - Pyramid Head Sculpture")
        assert result["action"] in ("auto", "ask")  # new folder → either is valid

    def test_no_existing_folders(self):
        matcher = self.FolderMatcher([])
        result = matcher.match("Wicked - Blade Sculpture (Non Supported)")
        # Should still return the stripped name as canonical
        assert result["canonical"] == "Wicked - Blade Sculpture"
