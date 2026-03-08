# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by Keep a Changelog and this project uses Semantic Versioning tags for public releases.

## [Unreleased]

### Added

- `repair-chunk` command for showing, patching, or retrying a single chunk without broad reruns
- prompt-template preflight validation for the high-level workflow
- explicit `book.translated_title` project metadata for localized final EPUB titles

### Fixed

- prompt-template rendering regression caused by adjacent placeholders in external prompt files
- path resolution for explicit QA snapshots, remediation plans, and batch output files
- local remediation crash in `apply-local-fixes` after the `source_language` refactor
- clearer batch-state and apply/download error handling for the most recent translation and QA runs
- `finalize` now syncs EPUB package title metadata from project config instead of leaving source-language titles in place

## [0.1.0] - 2026-03-08

First public release.

### Added

- configurable `source_language` in project configuration
- external prompt templates under `prompts/`
- high-level CLI commands: `draft`, `review`, and `finalize`
- deterministic remediation workflow commands:
  - `validate-local`
  - `build-remediation-plan`
  - `apply-local-fixes`
  - `retry-targeted`
  - `qa-changed`
  - `final-gate`
- lightweight regression tests in [`tests/test_epub_translate.py`](tests/test_epub_translate.py)
- English project documentation in [`README.md`](README.md) and [`MANUAL.md`](MANUAL.md)

### Changed

- moved translation and QA prompts out of Python into dedicated template files
- simplified the public-facing workflow to `draft -> review -> finalize`
- generalized the tool so it is no longer implicitly hardwired to French-only project configuration
- made final EPUB assembly explicit through `finalize`
- improved glossary/report wording from source-language-specific labels toward generic `Source`

### Fixed

- safer batch output application so a malformed chunk does not abort the whole apply step
- better local validation behavior around placeholder integrity and structural checks
- stronger deterministic local fixes for structural labels, spacing issues, and obvious source/target mismatches
- improved quality-gate handling for changed-chunk QA and remediation loops

### Notes

- This release focuses on making the repository usable by other people, not just the original book workflow.
- Cloud QA is still optional and should be used selectively to avoid unnecessary cost.
