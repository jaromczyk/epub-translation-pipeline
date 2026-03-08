import importlib.util
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "tools" / "epub_translate.py"


def load_module():
    spec = importlib.util.spec_from_file_location("epub_translate", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@contextmanager
def temporary_cwd(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


class EpubTranslateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_module()

    def test_default_project_config_sets_source_language(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir) / "sample-project"
            config = self.mod.default_project_config(
                Path(tmpdir) / "book.epub",
                project_dir,
                target_language="en",
                source_language="fr",
            )
            self.assertEqual(config["translation"]["source_language"], "fr")
            self.assertEqual(config["translation"]["target_language"], "en")

    def test_read_project_config_backfills_source_language(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir) / "sample-project"
            project_dir.mkdir(parents=True, exist_ok=True)
            self.mod.save_json(
                project_dir / "project.json",
                {
                    "book": {"source_epub": "book.epub", "project_slug": "sample-project"},
                    "translation": {"target_language": "pl"},
                    "openai": {},
                },
            )
            config = self.mod.read_project_config(project_dir)
            self.assertEqual(config["translation"]["source_language"], "fr")

    def test_prompt_templates_are_loaded_from_files(self):
        prompt = self.mod.build_system_prompt("en", source_language="fr")
        self.assertIn("English", prompt)
        self.assertIn("French", prompt)
        self.mod.validate_prompt_templates()

    def test_structural_translation_uses_source_profile(self):
        translated = self.mod.structural_translation("Sommaire", "fr", "pl")
        self.assertEqual(translated, "Spis treści")

    def test_resolve_existing_cli_path_supports_repo_relative_and_project_relative(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            project_dir = base / "project"
            project_dir.mkdir()
            repo_file = base / "repo-file.txt"
            project_file = project_dir / "project-file.txt"
            repo_file.write_text("repo", encoding="utf-8")
            project_file.write_text("project", encoding="utf-8")
            with temporary_cwd(base):
                resolved_repo = self.mod.resolve_existing_cli_path(project_dir, "repo-file.txt", "Test file")
                resolved_project = self.mod.resolve_existing_cli_path(project_dir, "project-file.txt", "Test file")
            self.assertEqual(resolved_repo, repo_file.resolve())
            self.assertEqual(resolved_project, project_file.resolve())

    def test_resolve_output_cli_path_uses_project_dir_for_bare_filenames(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            project_dir = base / "project"
            project_dir.mkdir()
            with temporary_cwd(base):
                resolved = self.mod.resolve_output_cli_path(project_dir, "plan.json")
                nested = self.mod.resolve_output_cli_path(project_dir, "out/plan.json")
            self.assertEqual(resolved, (project_dir / "plan.json").resolve())
            self.assertEqual(nested, (base / "out" / "plan.json").resolve())

    def test_load_batch_state_reads_latest_translation_and_qa_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir) / "project"
            batches = project_dir / "batches"
            batches.mkdir(parents=True)
            self.mod.save_json(batches / "last_batch.json", {"id": "batch-123"})
            self.mod.save_json(batches / "last_qa_batch.json", {"id": "qa-456"})
            state_path, state = self.mod.load_batch_state(project_dir, None, qa=False)
            qa_state_path, qa_state = self.mod.load_batch_state(project_dir, None, qa=True)
            self.assertEqual(state_path.name, "last_batch.json")
            self.assertEqual(state["id"], "batch-123")
            self.assertEqual(qa_state_path.name, "last_qa_batch.json")
            self.assertEqual(qa_state["id"], "qa-456")

    def test_parse_repair_assignment_supports_inline_and_file_values(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir) / "project"
            project_dir.mkdir()
            text_file = project_dir / "replacement.txt"
            text_file.write_text("patched text", encoding="utf-8")
            inline = self.mod.parse_repair_assignment("2=hello", project_dir, from_file=False)
            from_file = self.mod.parse_repair_assignment("3=replacement.txt", project_dir, from_file=True)
            self.assertEqual(inline, (2, "hello"))
            self.assertEqual(from_file, (3, "patched text"))

    def test_apply_chunk_replacements_updates_project_and_blocks_placeholder_mismatch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            project_dir = base / "sample-project"
            source_dir = project_dir / "workspace" / "unpacked" / "source"
            translated_dir = project_dir / "workspace" / "unpacked" / "translated"
            source_dir.mkdir(parents=True)
            translated_dir.mkdir(parents=True)
            href = "chapter.xhtml"
            source_file = source_dir / href
            source_file.write_text(
                """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"><body><p>Hello <em>world</em> and <strong>friends</strong>.</p></body></html>""",
                encoding="utf-8",
            )
            translated_file = translated_dir / href
            translated_file.write_text(source_file.read_text(encoding="utf-8"), encoding="utf-8")
            config = self.mod.default_project_config(base / "book.epub", project_dir, target_language="en", source_language="fr")
            self.mod.save_json(project_dir / "project.json", config)
            progress = self.mod.create_progress_stub(base / "book.epub")
            self.mod.save_json(project_dir / "workspace" / "progress.json", progress)
            paths = self.mod.project_paths(project_dir, target_language="en")
            tree, units = self.mod.collect_text_units(source_file)
            translated_lookup = {(target.xpath, target.field): target.original_text for unit in units for target in unit.targets}
            self.mod.save_translated_lookup_for_href(paths, progress, href, translated_lookup)
            self.mod.save_json(paths["progress"], progress)
            chunk = self.mod.load_chunk_from_source(paths, href, 0, config["translation"]["max_chars_per_chunk"])
            current = self.mod.render_unit_translation_with_placeholders(chunk[0], translated_lookup)
            result = self.mod.apply_chunk_replacements(
                project_dir,
                config,
                href,
                0,
                {0: current.replace("Hello", "Hi")},
            )
            self.assertEqual(result["changed_units"], [0])
            updated_progress = self.mod.load_json(paths["progress"], {})
            updated_lookup = self.mod.progress_lookup_for_href(updated_progress, href)
            self.assertIn("Hi", self.mod.render_unit_translation(chunk[0], updated_lookup))
            broken = current.replace("[[SEG_1]]", "")
            with self.assertRaises(RuntimeError):
                self.mod.apply_chunk_replacements(project_dir, config, href, 0, {0: broken})


if __name__ == "__main__":
    unittest.main()
