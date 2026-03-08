import importlib.util
import sys
import tempfile
import unittest
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

    def test_structural_translation_uses_source_profile(self):
        translated = self.mod.structural_translation("Sommaire", "fr", "pl")
        self.assertEqual(translated, "Spis treści")


if __name__ == "__main__":
    unittest.main()
