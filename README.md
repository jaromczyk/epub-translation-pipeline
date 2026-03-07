# EPUB Translation Pipeline

Python tool for translating EPUB books safely while preserving XHTML structure, links, IDs, notes, and packaging.

The repository contains the tool and documentation only. Book files, generated projects, translation workspaces, and local assets are intentionally excluded from version control.

## What It Does

- unpacks EPUB files
- finds content XHTML/HTML files in reading order
- translates only visible reader-facing text
- preserves tags, attributes, anchors, links, and EPUB structure
- splits long chapters into smaller chunks
- saves progress so translation can be resumed
- rebuilds translated EPUB output
- supports OpenAI direct mode and Batch API workflows
- supports lightweight cloud QA as a separate batch

## Main Files

- `tools/epub_translate.py`
- `MANUAL.md`

## Typical Workflow

1. Initialize a project from an EPUB.
2. Configure model and translation options.
3. Run a test chapter or full batch.
4. Download and apply results.
5. Run local cleanup and QA.
6. Optionally run cloud QA as a diagnostic pass.

See `MANUAL.md` for commands and examples.

## Requirements

- Python 3.11+
- `OPENAI_API_KEY` set in the shell

## Suggested Repository Scope

Keep only:

- the script
- documentation
- future helper modules/tests if added

Do not commit:

- EPUB books
- generated `projects/`
- translation caches
- batch outputs
- local cover assets
