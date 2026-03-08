# EPUB Translate Manual

Tool: `tools/epub_translate.py`

This tool translates EPUB books while preserving XHTML structure, navigation files, anchors, notes, and packaging. It supports draft translation, cheap local review, cloud QA, deterministic remediation, and final EPUB assembly.

## Requirements

- Python 3.11+
- `OPENAI_API_KEY` set in the shell
- a source `.epub` file

PowerShell:

```powershell
$env:OPENAI_API_KEY="your_key"
```

CMD:

```cmd
set OPENAI_API_KEY=your_key
```

## Project Convention

Each book should live in its own project under `projects/`.

Examples:

- `projects/my-book-en`
- `projects/my-book-pl`
- `projects/qu-est-ce-que-le-fascisme-maurice-bardeche-pl`

## Recommended Workflow

The intended low-cost flow is:

1. `draft`
2. `review`
3. optional cloud QA once, then remediation on the flagged subset
4. `finalize`

Avoid repeated broad retry loops. Use cloud QA as a focused diagnostic step, not as the default loop for the whole book.

## Core Commands

### 1. Initialize a project

```powershell
python tools\epub_translate.py init-project --epub "book.epub" --project-root projects --source-language fr --target-language en
```

Optional translated metadata title:

```powershell
python tools\epub_translate.py init-project --epub "book.epub" --project-root projects --source-language fr --target-language en --translated-title "My Book Title"
```

This creates:

- `project.json`
- `glossary.md`
- `QA.md`
- `workspace/`
- `batches/`

### 2. List content files

```powershell
python tools\epub_translate.py list-content --project my-book-en --project-root projects
```

### 3. Configure model and project language settings

```powershell
python tools\epub_translate.py configure-openai --project my-book-en --project-root projects --model gpt-5.4 --reasoning-effort none --use-batch --source-language fr --target-language en
```

Important options:

- `--model`
- `--temperature`
- `--reasoning-effort {none,low,medium,high}`
- `--use-batch` / `--no-use-batch`
- `--run-qa-after-apply` / `--no-run-qa-after-apply`
- `--source-language`
- `--target-language`
- `--translated-title`

### 4. Estimate translation cost

```powershell
python tools\epub_translate.py estimate-cost --project my-book-en --project-root projects
```

### 5. Suggest glossary candidates

```powershell
python tools\epub_translate.py suggest-glossary --project my-book-en --project-root projects
```

Optional chapter scope:

```powershell
python tools\epub_translate.py suggest-glossary --project my-book-en --project-root projects --chapter OEBPS/chapter01.xhtml --max-candidates 40
```

## High-Level Commands

### `draft`

Runs the default first-pass translation flow. If the project is configured for Batch API, it prepares and submits a batch. Otherwise it uses direct mode.

```powershell
python tools\epub_translate.py draft --project my-book-en --project-root projects
```

Optional chapter scope:

```powershell
python tools\epub_translate.py draft --project my-book-en --project-root projects --chapter OEBPS/chapter01.xhtml
```

### `review`

Runs only deterministic local validation. This is the cheap first review layer.

```powershell
python tools\epub_translate.py review --project my-book-en --project-root projects
```

Output is written to `QA_local.md`.

### `finalize`

Normalizes translated package metadata, syncs navigation files, and rebuilds the output EPUB.

```powershell
python tools\epub_translate.py finalize --project my-book-en --project-root projects
```

If `book.translated_title` is set in `project.json`, `finalize` also updates:

- `dc:title` in `content.opf`
- `calibre:title_sort` in `content.opf`
- `docTitle` in `toc.ncx`

If it is missing, `finalize` leaves the existing title metadata unchanged and emits a warning in its JSON output.

### `repair-chunk`

Use this when one chunk needs inspection, a manual patch, or a single emergency retry.

Show chunk context:

```powershell
python tools\epub_translate.py repair-chunk --project my-book-en --project-root projects --href OEBPS\chapter01.xhtml --chunk-index 3 --mode show
```

Apply one or more manual replacements:

```powershell
python tools\epub_translate.py repair-chunk --project my-book-en --project-root projects --href OEBPS\chapter01.xhtml --chunk-index 3 --mode apply --set "0=Corrected text with [[SEG_1]] placeholder"
```

Apply long replacement text from a file:

```powershell
python tools\epub_translate.py repair-chunk --project my-book-en --project-root projects --href OEBPS\chapter01.xhtml --chunk-index 3 --mode apply --set-file "0=patches\chunk3_unit0.txt"
```

Retry exactly one chunk through the model:

```powershell
python tools\epub_translate.py repair-chunk --project my-book-en --project-root projects --href OEBPS\chapter01.xhtml --chunk-index 3 --mode retry-single
```

`repair-chunk` fails loudly on placeholder mismatch and does not partially write a broken chunk.

## Low-Level Translation Commands

Use these when you need direct control over the pipeline.

### Prepare translation batch

```powershell
python tools\epub_translate.py prepare-batch --project my-book-en --project-root projects
```

### Prepare and submit translation batch

```powershell
python tools\epub_translate.py run-batch --project my-book-en --project-root projects
```

### Direct translation mode

```powershell
python tools\epub_translate.py translate-direct --project my-book-en --project-root projects
```

### Batch status / download / apply

```powershell
python tools\epub_translate.py batch-status --project my-book-en --project-root projects
python tools\epub_translate.py batch-download-output --project my-book-en --project-root projects
python tools\epub_translate.py apply-batch-output --project my-book-en --project-root projects --skip-qa
```

If you pass an explicit file path to `apply-batch-output` or `apply-qa-output`, the tool now accepts:

- an absolute path
- a repo-relative path such as `projects\my-book-en\batches\output.jsonl`
- a project-relative file only when it actually exists inside the project

## QA Commands

### Local validation

```powershell
python tools\epub_translate.py validate-local --project my-book-en --project-root projects
```

### Estimate cloud QA cost

```powershell
python tools\epub_translate.py estimate-qa-cost --project my-book-en --project-root projects
```

### Cloud QA batch

```powershell
python tools\epub_translate.py run-qa-batch --project my-book-en --project-root projects
python tools\epub_translate.py qa-batch-status --project my-book-en --project-root projects
python tools\epub_translate.py qa-batch-download-output --project my-book-en --project-root projects
python tools\epub_translate.py apply-qa-output --project my-book-en --project-root projects
```

Cloud QA writes:

- `QA_cloud.md`
- `QA_cloud_history/YYYY-MM-DD_<scope>.md`

## Remediation Commands

These commands support the deterministic `draft -> QA -> final` flow without broad reruns.

Recommended cost discipline:

1. `draft`
2. optional `run-qa-batch` once
3. `build-remediation-plan`
4. `apply-local-fixes`
5. `repair-chunk` for single high-risk passages
6. `finalize`

### Build a frozen remediation plan

```powershell
python tools\epub_translate.py build-remediation-plan --project my-book-en --project-root projects --qa-snapshot projects\my-book-en\QA_cloud_history\2026-03-08_full.md
```

### Apply local fixes

```powershell
python tools\epub_translate.py apply-local-fixes --project my-book-en --project-root projects
```

### Retry only model-fix chunks

```powershell
python tools\epub_translate.py retry-targeted --project my-book-en --project-root projects --max-chunks 5
```

### QA only changed chunks

```powershell
python tools\epub_translate.py qa-changed --project my-book-en --project-root projects
```

### Final gate

```powershell
python tools\epub_translate.py final-gate --project my-book-en --project-root projects
```

## Prompt Templates

Prompt bodies live outside Python in `prompts/`:

- `translation_system.txt`
- `translation_user.txt`
- `qa_system.txt`
- `qa_user.txt`

If you want to tune behavior for public use, change these files first instead of editing prompt strings inside the script.

## Output Files

Typical important project files:

- `project.json`
- `glossary.md`
- `glossary_suggestions.md`
- `QA.md`
- `QA_local.md`
- `QA_cloud.md`
- `QA_changed.md`
- `remediation_plan.json`
- `workspace/progress.json`
- `<project>_<lang>.epub`

## Notes for Public/Shared Use

- `source_language` is configurable and should be set explicitly per project.
- Batch mode is usually the cheapest default for full-book draft translation.
- `reasoning_effort none` is a sensible default for draft and QA.
- Targeted retries should stay small. Do not rerun the whole book unless you intentionally want a fresh draft.
- Prefer `repair-chunk` for one-off emergency fixes instead of ad-hoc scripts or broad reruns.

## Tests

```powershell
python -m unittest discover -s tests
```
