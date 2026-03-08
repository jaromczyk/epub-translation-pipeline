# EPUB Translation Pipeline

`tools/epub_translate.py` translates EPUB books while preserving XHTML structure, links, notes, and packaging.

The repository is intentionally small. It contains the tool, prompt templates, tests, and docs. Generated book projects, source EPUBs, batch outputs, and local assets stay out of version control.

Current public release: `v0.1.0`

- changelog: [`CHANGELOG.md`](CHANGELOG.md)

## Highlights

- source-language aware project config
- external prompt templates in [`prompts/`](prompts/)
- resumable draft translation via direct mode or Batch API
- local validation, remediation manifests, and targeted retry flow
- explicit `repair-chunk` command for high-risk manual or single-chunk recovery work
- explicit `finalize` command to rebuild the output EPUB

## Happy Path

1. Create a project:

```powershell
python tools\epub_translate.py init-project --epub "book.epub" --project-root projects --source-language fr --target-language en
```

2. Configure the model:

```powershell
python tools\epub_translate.py configure-openai --project my-book-en --project-root projects --model gpt-5.4 --reasoning-effort none --use-batch
```

3. Run the draft translation:

```powershell
python tools\epub_translate.py draft --project my-book-en --project-root projects
```

4. Run cheap local review:

```powershell
python tools\epub_translate.py review --project my-book-en --project-root projects
```

5. If needed, use the remediation commands:

- `build-remediation-plan`
- `apply-local-fixes`
- `repair-chunk`
- `retry-targeted`
- `qa-changed`
- `final-gate`

6. Build the final EPUB:

```powershell
python tools\epub_translate.py finalize --project my-book-en --project-root projects
```

## Repository Layout

- [`tools/epub_translate.py`](tools/epub_translate.py)
- [`prompts/translation_system.txt`](prompts/translation_system.txt)
- [`prompts/translation_user.txt`](prompts/translation_user.txt)
- [`prompts/qa_system.txt`](prompts/qa_system.txt)
- [`prompts/qa_user.txt`](prompts/qa_user.txt)
- [`MANUAL.md`](MANUAL.md)
- [`tests/test_epub_translate.py`](tests/test_epub_translate.py)

## Requirements

- Python 3.11+
- `OPENAI_API_KEY` in the shell

## Tests

```powershell
python -m unittest discover -s tests
```
