#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import shutil
import sys
import unicodedata
import urllib.error
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET


XHTML_NS = "http://www.w3.org/1999/xhtml"
OPF_NS = "http://www.idpf.org/2007/opf"
EPUB_NS = "http://www.idpf.org/2007/ops"
NS = {"xhtml": XHTML_NS, "opf": OPF_NS}
ET.register_namespace("", XHTML_NS)
ET.register_namespace("epub", EPUB_NS)

IGNORE_TAGS = {"script", "style", "title", "meta", "link", "head"}
BLOCK_TAGS = {
    "p",
    "li",
    "dd",
    "dt",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "td",
    "th",
    "figcaption",
    "caption",
    "blockquote",
    "pre",
    "div",
}
FRENCH_MARKERS = {
    " le ",
    " la ",
    " les ",
    " des ",
    " une ",
    " un ",
    " que ",
    " pour ",
    " avec ",
    " nous ",
    " dans ",
}
PRICE_TABLE = {
    "gpt-5.4": {"input": 2.50, "output": 15.00},
    "gpt-5-mini": {"input": 0.25, "output": 2.00},
    "gpt-5": {"input": 1.25, "output": 10.00},
}
LANGUAGE_NAMES = {
    "en": "English",
    "pl": "Polish",
}
FRENCH_STOPWORDS = {
    "a",
    "au",
    "aux",
    "ce",
    "ces",
    "cet",
    "cette",
    "comme",
    "dans",
    "de",
    "des",
    "du",
    "en",
    "et",
    "il",
    "ils",
    "la",
    "le",
    "les",
    "leur",
    "leurs",
    "mais",
    "ne",
    "nous",
    "ou",
    "par",
    "pas",
    "plus",
    "pour",
    "que",
    "qui",
    "sa",
    "se",
    "ses",
    "son",
    "sur",
    "un",
    "une",
    "vos",
    "vous",
}


def target_language_name(code: str) -> str:
    normalized = (code or "en").split("-", 1)[0].lower()
    return LANGUAGE_NAMES.get(normalized, normalized)


def build_system_prompt(target_language: str) -> str:
    language_name = target_language_name(target_language)
    return f"""Translate like a professional literary translator of historical and political essays.
Preserve meaning with high fidelity and produce natural, elegant book {language_name}.

Style requirements:
- preserve the author's serious, analytical, essayistic tone,
- do not simplify arguments or compress the reasoning,
- preserve rhetoric and the structure of the argument,
- prefer literary, bookish phrasing over colloquial wording,
- keep longer sentences where they remain natural in {language_name},
- do not soften controversial or sharp formulations,
- do not add commentary or interpretation,
- preserve quotations, footnotes, headings, and emphasis,
- preserve proper names, institutions, historical events, and legal terminology consistently.

Quality requirements:
- do not leave French words untranslated unless they are clearly being cited as French or are standard in {language_name},
- do not produce hybrid phrases such as half-translated idioms or unexplained gallicisms,
- do not leave culturally marked French common nouns such as food names, slang, or metaphoric expressions untranslated unless the context clearly requires retention,
- when a literal rendering sounds awkward in {language_name}, choose an idiomatic literary equivalent that preserves meaning and tone,
- avoid duplicated determiners, duplicated words, or broken syntax,
- if a foreign or italicized expression is intentionally retained, integrate it grammatically into the target sentence,
- do not produce doubled forms such as a retained foreign word immediately followed by a redundant translated gloss of the same word,
- preserve existing italics and emphasis semantically; translate the emphasized words unless they are titles or fixed foreign expressions,
- keep political and historical terminology consistent across the book.
"""


@dataclass
class TextTarget:
    xpath: str
    field: str
    original_text: str


@dataclass
class TextUnit:
    xpath: str
    field: str
    text: str
    plain_text: str
    targets: List[TextTarget]


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, data) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    counter = 2
    while True:
        candidate = path.with_name(f"{path.stem}_{counter}{path.suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_text).strip("-").lower()
    return slug or "book"


def project_paths(project_dir: Path, target_language: Optional[str] = None) -> Dict[str, Path]:
    slug = project_dir.name
    workspace = project_dir / "workspace"
    config_path = project_dir / "project.json"
    configured_output_epub: Optional[Path] = None
    if target_language is None and config_path.exists():
        try:
            config = load_json(config_path, {})
            target_language = config.get("translation", {}).get("target_language")
            output_epub = config.get("book", {}).get("output_epub")
            if output_epub:
                configured_output_epub = Path(output_epub)
        except Exception:
            target_language = None
    normalized_lang = (target_language or "en").split("-", 1)[0].lower()
    return {
        "project_dir": project_dir,
        "config": project_dir / "project.json",
        "glossary": project_dir / "glossary.md",
        "glossary_suggestions": project_dir / "glossary_suggestions.md",
        "qa": project_dir / "QA.md",
        "qa_cloud": project_dir / "QA_cloud.md",
        "qa_cloud_history": project_dir / "QA_cloud_history",
        "progress": workspace / "progress.json",
        "batch_requests": project_dir / "batches" / "requests.jsonl",
        "batch_map": project_dir / "batches" / "requests_map.json",
        "batch_output": project_dir / "batches" / "output.jsonl",
        "qa_batch_requests": project_dir / "batches" / "qa_requests.jsonl",
        "qa_batch_map": project_dir / "batches" / "qa_requests_map.json",
        "qa_batch_output": project_dir / "batches" / "qa_output.jsonl",
        "qa_last_batch": project_dir / "batches" / "last_qa_batch.json",
        "source_dir": workspace / "unpacked" / "source",
        "translated_dir": workspace / "unpacked" / "translated",
        "output_epub": configured_output_epub or (project_dir / f"{slug}_{normalized_lang}.epub"),
    }


def default_project_config(epub_path: Path, project_dir: Path, target_language: str = "en") -> Dict:
    paths = project_paths(project_dir, target_language=target_language)
    return {
        "book": {
            "source_epub": str(epub_path.resolve()),
            "project_slug": project_dir.name,
            "output_epub": str(paths["output_epub"].resolve()),
        },
        "translation": {
            "target_language": target_language,
            "max_chars_per_chunk": 3200,
            "preserve_visible_text_only": True,
            "run_qa_after_apply": True,
        },
        "openai": {
            "endpoint": "/v1/responses",
            "use_batch_api": True,
            "completion_window": "24h",
            "model": None,
            "temperature": 0.2,
            "send_temperature": True,
            "reasoning_effort": "medium",
        },
        "paths": {
            "glossary": str(paths["glossary"].resolve()),
            "qa": str(paths["qa"].resolve()),
            "progress": str(paths["progress"].resolve()),
            "batch_requests": str(paths["batch_requests"].resolve()),
            "batch_map": str(paths["batch_map"].resolve()),
            "batch_output": str(paths["batch_output"].resolve()),
        },
    }


def write_default_glossary(glossary_path: Path, target_language: str = "en") -> None:
    if glossary_path.exists():
        return
    language_name = target_language_name(target_language)
    glossary_path.write_text(
        f"# Glossary\n\n| French | {language_name} |\n| --- | --- |\n",
        encoding="utf-8",
    )


def write_default_qa(qa_path: Path) -> None:
    if qa_path.exists():
        return
    qa_path.write_text("# QA Report\n\nPending.\n", encoding="utf-8")


def read_project_config(project_dir: Path) -> Dict:
    config_path = project_paths(project_dir)["config"]
    if not config_path.exists():
        raise RuntimeError(f"Project config not found: {config_path}")
    return load_json(config_path, {})


def save_project_config(project_dir: Path, config: Dict) -> None:
    save_json(project_paths(project_dir)["config"], config)


def unzip_epub(epub_path: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    ensure_dir(destination)
    with zipfile.ZipFile(epub_path) as zf:
        zf.extractall(destination)


def rezip_epub(source_dir: Path, output_epub: Path) -> None:
    if output_epub.exists():
        output_epub.unlink()
    with zipfile.ZipFile(output_epub, "w") as zf:
        mimetype = source_dir / "mimetype"
        if mimetype.exists():
            zf.write(mimetype, "mimetype", compress_type=zipfile.ZIP_STORED)
        for file_path in sorted(source_dir.rglob("*")):
            if file_path.is_dir() or file_path == mimetype:
                continue
            zf.write(
                file_path,
                file_path.relative_to(source_dir).as_posix(),
                compress_type=zipfile.ZIP_DEFLATED,
            )


def parse_opf(opf_path: Path) -> Tuple[List[str], Dict[str, str]]:
    tree = ET.parse(opf_path)
    root = tree.getroot()
    manifest = {}
    for item in root.findall(".//opf:manifest/opf:item", NS):
        manifest[item.attrib["id"]] = item.attrib["href"]
    spine = []
    for itemref in root.findall(".//opf:spine/opf:itemref", NS):
        href = manifest.get(itemref.attrib["idref"])
        if href:
            spine.append(href)
    guide_titles = {}
    for ref in root.findall(".//opf:guide/opf:reference", NS):
        guide_titles[ref.attrib.get("href", "")] = ref.attrib.get("title", "")
    return spine, guide_titles


def find_opf_path(unpacked_dir: Path) -> Path:
    container_path = unpacked_dir / "META-INF" / "container.xml"
    if container_path.exists():
        tree = ET.parse(container_path)
        root = tree.getroot()
        rootfile = root.find(".//{*}rootfile")
        if rootfile is not None:
            full_path = rootfile.attrib.get("full-path")
            if full_path:
                return unpacked_dir / Path(full_path)
    fallback = unpacked_dir / "content.opf"
    if fallback.exists():
        return fallback
    raise RuntimeError(f"Could not locate OPF package file in {unpacked_dir}")


def visible_content_files(unpacked_dir: Path) -> List[str]:
    opf_path = find_opf_path(unpacked_dir)
    spine, _ = parse_opf(opf_path)
    base_dir = opf_path.parent
    files = []
    for href in spine:
        if href.lower().endswith((".xhtml", ".html", ".htm")):
            files.append((base_dir / href).relative_to(unpacked_dir).as_posix())
    for extra_name in ("toc.xhtml",):
        for extra_path in unpacked_dir.rglob(extra_name):
            rel = extra_path.relative_to(unpacked_dir).as_posix()
            if rel not in files:
                files.append(rel)
    return files


def node_xpath(node: ET.Element, parents: Dict[int, Optional[ET.Element]]) -> str:
    parts: List[str] = []
    current: Optional[ET.Element] = node
    while current is not None:
        parent = parents.get(id(current))
        tag = strip_ns(current.tag)
        if parent is None:
            parts.append(tag)
        else:
            siblings = [child for child in list(parent) if strip_ns(child.tag) == tag]
            parts.append(f"{tag}[{siblings.index(current) + 1}]")
        current = parent
    return "/" + "/".join(reversed(parts))


def is_pagebreak_element(elem: ET.Element) -> bool:
    return strip_ns(elem.tag) == "span" and elem.attrib.get(f"{{{EPUB_NS}}}type") == "pagebreak"


def is_block_candidate(elem: ET.Element) -> bool:
    return strip_ns(elem.tag) in BLOCK_TAGS


def has_nested_block_candidate(elem: ET.Element) -> bool:
    for child in elem.iter():
        if child is elem:
            continue
        if is_block_candidate(child):
            return True
    return False


def segment_placeholder(index: int) -> str:
    return f"[[SEG_{index}]]"


def build_unit_source_text(targets: List[TextTarget]) -> str:
    parts: List[str] = []
    for index, target in enumerate(targets):
        parts.append(normalize_space(target.original_text))
        if index < len(targets) - 1:
            parts.append(segment_placeholder(index + 1))
    return "".join(parts).strip()


def build_unit_plain_text(targets: List[TextTarget]) -> str:
    return normalize_space("".join(target.original_text for target in targets))


def collect_targets_for_block(
    elem: ET.Element,
    parents: Dict[int, Optional[ET.Element]],
) -> List[TextTarget]:
    targets: List[TextTarget] = []

    def walk(node: ET.Element) -> None:
        if strip_ns(node.tag) in IGNORE_TAGS or is_pagebreak_element(node):
            return
        xpath = node_xpath(node, parents)
        if node.text and normalize_space(node.text):
            targets.append(TextTarget(xpath=xpath, field="text", original_text=node.text))
        for idx, child in enumerate(list(node), start=1):
            walk(child)
            if child.tail and normalize_space(child.tail):
                targets.append(TextTarget(xpath=f"{xpath}/tail[{idx}]", field="tail", original_text=child.tail))

    walk(elem)
    return targets


def collect_text_units(xhtml_path: Path) -> Tuple[ET.ElementTree, List[TextUnit]]:
    tree = ET.parse(xhtml_path)
    root = tree.getroot()
    parents: Dict[int, Optional[ET.Element]] = {id(root): None}
    for parent in root.iter():
        for child in list(parent):
            parents[id(child)] = parent
    body = root.find(".//xhtml:body", NS)
    if body is None:
        return tree, []

    units: List[TextUnit] = []
    for elem in body.iter():
        if strip_ns(elem.tag) in IGNORE_TAGS or not is_block_candidate(elem):
            continue
        if has_nested_block_candidate(elem):
            continue
        targets = collect_targets_for_block(elem, parents)
        if not targets:
            continue
        units.append(
            TextUnit(
                xpath=targets[0].xpath,
                field=targets[0].field,
                text=build_unit_source_text(targets),
                plain_text=build_unit_plain_text(targets),
                targets=targets,
            )
        )
    return tree, units


def chunk_units(units: List[TextUnit], max_chars: int) -> List[List[TextUnit]]:
    chunks: List[List[TextUnit]] = []
    current: List[TextUnit] = []
    current_chars = 0
    for unit in units:
        unit_chars = len(unit.plain_text)
        if current and current_chars + unit_chars > max_chars:
            chunks.append(current)
            current = []
            current_chars = 0
        current.append(unit)
        current_chars += unit_chars
    if current:
        chunks.append(current)
    return chunks


def preserve_outer_whitespace(original: str, translated: str) -> str:
    match = re.match(r"^(\s*)(.*?)(\s*)$", original, re.DOTALL)
    if not match:
        return translated
    return f"{match.group(1)}{translated}{match.group(3)}"


def split_translated_text(unit: TextUnit, translated_text: str) -> List[str]:
    if len(unit.targets) == 1:
        return [preserve_outer_whitespace(unit.targets[0].original_text, translated_text)]
    expected_placeholders = [segment_placeholder(index) for index in range(1, len(unit.targets))]
    actual_placeholders = re.findall(r"\[\[SEG_(\d+)\]\]", translated_text)
    expected_ids = [str(index) for index in range(1, len(unit.targets))]
    if actual_placeholders != expected_ids:
        raise RuntimeError(
            f"Placeholder mismatch for unit {unit.xpath}: expected {expected_placeholders}, got "
            f"{[segment_placeholder(int(index)) for index in actual_placeholders]}"
        )
    pattern = "|".join(re.escape(token) for token in expected_placeholders)
    parts = re.split(pattern, translated_text)
    if len(parts) != len(unit.targets):
        raise RuntimeError(f"Could not split translated text for unit {unit.xpath} into {len(unit.targets)} parts.")
    return [
        preserve_outer_whitespace(target.original_text, piece)
        for target, piece in zip(unit.targets, parts)
    ]


def assign_translations(tree: ET.ElementTree, translated: Dict[Tuple[str, str], str]) -> None:
    root = tree.getroot()
    parents: Dict[int, Optional[ET.Element]] = {id(root): None}
    for parent in root.iter():
        for child in list(parent):
            parents[id(child)] = parent
    body = root.find(".//xhtml:body", NS)
    if body is None:
        return
    for elem in body.iter():
        if strip_ns(elem.tag) in IGNORE_TAGS:
            continue
        xpath = node_xpath(elem, parents)
        key = (xpath, "text")
        if key in translated:
            elem.text = translated[key]
        for idx, child in enumerate(list(elem), start=1):
            key = (f"{xpath}/tail[{idx}]", "tail")
            if key in translated:
                child.tail = translated[key]


def render_unit_translation(unit: TextUnit, translated_lookup: Dict[Tuple[str, str], str]) -> str:
    return normalize_space("".join(translated_lookup.get((target.xpath, target.field), "") for target in unit.targets))


def load_glossary(glossary_path: Path) -> Dict[str, str]:
    glossary = {}
    if not glossary_path.exists():
        return glossary
    for line in glossary_path.read_text(encoding="utf-8").splitlines():
        if "|" not in line or line.startswith("| French"):
            continue
        parts = [part.strip() for part in line.strip("|").split("|")]
        if len(parts) >= 2 and parts[0] and parts[1]:
            glossary[parts[0]] = parts[1]
    return glossary


def extract_inline_glossary_candidates(tree: ET.ElementTree) -> List[str]:
    root = tree.getroot()
    body = root.find(".//xhtml:body", NS)
    if body is None:
        return []
    candidates: List[str] = []
    for elem in body.iter():
        if strip_ns(elem.tag) not in {"em", "i", "cite", "strong"}:
            continue
        text = normalize_space("".join(elem.itertext()))
        words = re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'’-]*", text)
        if 1 <= len(words) <= 6 and len(text) >= 3:
            candidates.append(text)
    return candidates


def extract_named_entity_candidates(text: str) -> List[str]:
    pattern = re.compile(
        r"\b([A-ZÀ-ÖØ-Þ][A-Za-zÀ-ÿ'’-]+(?:\s+(?:d['’]|de|du|des|la|le|les|et))?(?:\s+[A-ZÀ-ÖØ-Þ][A-Za-zÀ-ÿ'’-]+){0,3})\b"
    )
    candidates: List[str] = []
    for match in pattern.finditer(text):
        phrase = normalize_space(match.group(1))
        words = re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'’-]*", phrase)
        if not words:
            continue
        if len(words) == 1 and words[0].lower() in FRENCH_STOPWORDS:
            continue
        candidates.append(phrase)
    return candidates


def extract_repeated_term_candidates(text: str) -> List[str]:
    words = re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'’-]*", text)
    candidates: List[str] = []
    max_start = max(0, len(words) - 1)
    for size in (2, 3):
        for start in range(0, max(0, len(words) - size + 1)):
            phrase_words = words[start:start + size]
            lowered = [word.lower() for word in phrase_words]
            if lowered[0] in FRENCH_STOPWORDS or lowered[-1] in FRENCH_STOPWORDS:
                continue
            if all(word in FRENCH_STOPWORDS for word in lowered):
                continue
            phrase = " ".join(phrase_words)
            if len(phrase) < 8:
                continue
            candidates.append(phrase)
    return candidates


def suggest_glossary_candidates(
    source_dir: Path,
    files: List[str],
    existing_glossary: Dict[str, str],
    max_candidates: int = 80,
) -> List[Dict[str, str]]:
    existing_keys = {normalize_space(key).casefold() for key in existing_glossary}
    counts: Dict[str, Dict[str, object]] = {}

    def record(phrase: str, reason: str, href: str) -> None:
        normalized = normalize_space(phrase).strip(" ,;:.!?\"'()[]{}")
        if len(normalized) < 3:
            return
        key = normalized.casefold()
        if key in existing_keys:
            return
        entry = counts.setdefault(
            normalized,
            {"count": 0, "reasons": set(), "files": set()},
        )
        entry["count"] = int(entry["count"]) + 1
        cast_reasons = entry["reasons"]
        cast_files = entry["files"]
        if isinstance(cast_reasons, set):
            cast_reasons.add(reason)
        if isinstance(cast_files, set):
            cast_files.add(href)

    for href in files:
        source_file = source_dir / href
        if not source_file.exists():
            continue
        tree, units = collect_text_units(source_file)
        plain_text = " ".join(unit.plain_text for unit in units)
        for phrase in extract_inline_glossary_candidates(tree):
            record(phrase, "inline emphasis", href)
        for phrase in extract_named_entity_candidates(plain_text):
            record(phrase, "capitalized phrase", href)
        for phrase in extract_repeated_term_candidates(plain_text):
            record(phrase, "repeated term", href)

    ranked = []
    for phrase, data in counts.items():
        count = int(data["count"])
        reasons = sorted(data["reasons"]) if isinstance(data["reasons"], set) else []
        files_seen = sorted(data["files"]) if isinstance(data["files"], set) else []
        if count < 2 and "inline emphasis" not in reasons:
            continue
        ranked.append(
            {
                "source": phrase,
                "count": str(count),
                "reason": ", ".join(reasons[:2]),
                "files": ", ".join(files_seen[:3]),
            }
        )
    ranked.sort(key=lambda item: (-int(item["count"]), len(item["source"]), item["source"].casefold()))
    return ranked[:max_candidates]


def write_glossary_suggestions(
    suggestions_path: Path,
    project_name: str,
    target_language: str,
    suggestions: List[Dict[str, str]],
) -> None:
    language_name = target_language_name(target_language)
    lines = [
        "# Glossary Suggestions",
        "",
        f"Project: `{project_name}`",
        "",
        f"| French | Suggested {language_name} | Count | Why | Seen In |",
        "| --- | --- | --- | --- | --- |",
    ]
    if not suggestions:
        lines.append("| _No candidates_ |  |  |  |  |")
    else:
        for item in suggestions:
            source = item["source"].replace("|", "\\|")
            reason = item["reason"].replace("|", "\\|")
            files = item["files"].replace("|", "\\|")
            lines.append(f"| {source} |  | {item['count']} | {reason} | {files} |")
    suggestions_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_qa_feedback(qa_cloud_path: Path) -> Dict[str, Dict[int, Dict]]:
    if not qa_cloud_path.exists():
        return {}

    feedback: Dict[str, Dict[int, Dict]] = {}
    current_href: Optional[str] = None
    current_chunk: Optional[int] = None
    current_issue: Optional[Dict[str, str]] = None

    def ensure_current_entry() -> Optional[Dict]:
        if current_href is None or current_chunk is None:
            return None
        return feedback.setdefault(current_href, {}).setdefault(current_chunk, {"summary": "", "issues": []})

    for raw_line in qa_cloud_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        header_match = re.match(r"^###\s+(.+?)\s+chunk\s+(\d+)$", line)
        if header_match:
            current_href = header_match.group(1)
            current_chunk = int(header_match.group(2))
            current_issue = None
            ensure_current_entry()
            continue
        if current_href is None or current_chunk is None:
            continue
        if line.startswith("- error:"):
            feedback.setdefault(current_href, {}).pop(current_chunk, None)
            current_href = None
            current_chunk = None
            current_issue = None
            continue
        entry = ensure_current_entry()
        if entry is None:
            continue
        if line.startswith("- summary:"):
            entry["summary"] = line.split(":", 1)[1].strip()
            continue
        issue_match = re.match(r"^- `([^`]+)` `([^`]+)`: (.+)$", line)
        if issue_match:
            current_issue = {
                "severity": issue_match.group(1),
                "category": issue_match.group(2),
                "note": issue_match.group(3).strip(),
                "source_excerpt": "",
                "translation_excerpt": "",
            }
            entry["issues"].append(current_issue)
            continue
        if current_issue and line.startswith("- source:"):
            current_issue["source_excerpt"] = line.split(":", 1)[1].strip()
            continue
        if current_issue and line.startswith("- translation:"):
            current_issue["translation_excerpt"] = line.split(":", 1)[1].strip()
            continue
    return feedback


def build_qa_feedback_text(chunk_feedback: Optional[Dict]) -> str:
    if not chunk_feedback:
        return ""
    issues = chunk_feedback.get("issues") or []
    if not issues:
        return ""
    lines = [
        "Previous QA findings for this chunk:",
        f"- summary: {chunk_feedback.get('summary') or 'Previous translation had meaningful issues.'}",
        "- Fix the problems below in this retranslation.",
        "- Do not preserve broken phrasing from the previous attempt.",
    ]
    for issue in issues[:5]:
        lines.append(
            f"- [{issue.get('severity', 'unknown')}/{issue.get('category', 'unknown')}] {issue.get('note', '').strip()}"
        )
        source_excerpt = normalize_space(issue.get("source_excerpt", ""))
        translation_excerpt = normalize_space(issue.get("translation_excerpt", ""))
        if source_excerpt:
            lines.append(f"  source excerpt: {source_excerpt}")
        if translation_excerpt:
            lines.append(f"  previous translation excerpt: {translation_excerpt}")
    return "\n".join(lines)


def chunk_has_high_issue(chunk_feedback: Optional[Dict]) -> bool:
    if not chunk_feedback:
        return False
    return any(issue.get("severity") == "high" for issue in chunk_feedback.get("issues") or [])


def chunk_has_high_formatting_issue(chunk_feedback: Optional[Dict]) -> bool:
    if not chunk_feedback:
        return False
    return any(
        issue.get("severity") == "high" and issue.get("category") == "formatting"
        for issue in chunk_feedback.get("issues") or []
    )


def should_translate_chunk(
    chunk_index: int,
    completed: set[int],
    qa_feedback_for_file: Dict[int, Dict],
    retry_only_high: bool = False,
) -> bool:
    if chunk_index not in completed:
        return True
    if chunk_index not in qa_feedback_for_file:
        return False
    if not retry_only_high:
        return True
    return chunk_has_high_issue(qa_feedback_for_file.get(chunk_index))


def build_qa_history_filename(requests: List[Dict], now: Optional[dt.datetime] = None) -> str:
    stamp = (now or dt.datetime.now()).strftime("%Y-%m-%d")
    hrefs = sorted({request.get("href") for request in requests if request.get("href")})
    if not hrefs:
        scope = "empty"
    elif len(hrefs) == 1:
        stem = Path(hrefs[0]).stem
        match = re.search(r"(c\d+|p\d+(?:-st\d+)?|ftn\d+|tp\d+|toc\d+|cop\d+|acheve_\d+|toc)$", stem)
        scope = match.group(1) if match else stem
    else:
        scope = "full"
    scope = re.sub(r"[^a-zA-Z0-9._-]+", "-", scope).strip("-") or "report"
    return f"{stamp}_{scope}.md"


def generate_qa_report(translated_files: Iterable[Path], glossary: Dict[str, str], qa_path: Path) -> None:
    lines = ["# QA Report", "", "## Checks", ""]
    for file_path in translated_files:
        status = {
            "xml_ok": "PASS",
            "french_leftovers": "PASS",
            "formatting": "PASS",
            "terminology": "PASS",
            "sentence_breaks": "PASS",
        }
        notes: List[str] = []
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
        except ET.ParseError as exc:
            status["xml_ok"] = "FAIL"
            notes.append(f"XHTML parse error: {exc}")
            root = None
        if root is not None:
            text = " ".join(normalize_space("".join(elem.itertext())) for elem in root.iter())
            lowered = f" {text.lower()} "
            if sum(marker in lowered for marker in FRENCH_MARKERS) >= 3:
                status["french_leftovers"] = "WARN"
                notes.append("French function-word heuristics detected residual French text.")
            for fr, en in glossary.items():
                if fr in text and en not in text:
                    status["terminology"] = "WARN"
                    notes.append(f"Glossary term '{fr}' remains without '{en}'.")
                    break
            for paragraph in root.findall(".//xhtml:p", NS):
                content = normalize_space("".join(paragraph.itertext()))
                if len(content) >= 80 and not re.search(r'[.!?:"”’\)]$', content):
                    status["sentence_breaks"] = "WARN"
                    notes.append("At least one paragraph may end abruptly.")
                    break
        lines.append(f"### {file_path.name}")
        for key, value in status.items():
            lines.append(f"- {key}: {value}")
        lines.extend([f"- note: {note}" for note in notes] or ["- note: No issues detected by heuristic checks."])
        lines.append("")
    qa_path.write_text("\n".join(lines), encoding="utf-8")


def qa_pairs_for_chunk(source_chunk: List[TextUnit], translated_lookup: Dict[Tuple[str, str], str]) -> List[Dict[str, str]]:
    pairs: List[Dict[str, str]] = []
    for index, unit in enumerate(source_chunk):
        pairs.append(
            {
                "id": str(index),
                "source": unit.plain_text,
                "translation": render_unit_translation(unit, translated_lookup),
            }
        )
    return pairs


def build_qa_requests(
    project_dir: Path,
    config: Dict,
    selected_files: List[str],
    max_chars: int,
) -> Tuple[List[str], Dict, int, int]:
    paths = project_paths(project_dir)
    source_dir = paths["source_dir"]
    translated_dir = paths["translated_dir"]
    model = config["openai"].get("model")
    if not model:
        raise RuntimeError("Set a model first with configure-openai.")
    requests: List[str] = []
    request_map = {"project": project_dir.name, "requests": []}
    total_source_chars = 0
    total_translation_chars = 0

    for href in selected_files:
        source_file = source_dir / href
        translated_file = translated_dir / href
        if not source_file.exists() or not translated_file.exists():
            continue
        _, source_units = collect_text_units(source_file)
        _, translated_units = collect_text_units(translated_file)
        translated_lookup = {
            (target.xpath, target.field): target.original_text
            for unit in translated_units
            for target in unit.targets
        }
        chunks = chunk_units(source_units, max_chars)
        for chunk_index, chunk in enumerate(chunks):
            pairs = qa_pairs_for_chunk(chunk, translated_lookup)
            total_source_chars += sum(len(pair["source"]) for pair in pairs)
            total_translation_chars += sum(len(pair["translation"]) for pair in pairs)
            custom_id = f"{project_dir.name}:{href}:qa:{chunk_index:04d}"
            body = {
                "model": model,
                "input": build_qa_payload(pairs, config["translation"].get("target_language", "en")),
            }
            request_line = {
                "custom_id": custom_id,
                "method": "POST",
                "url": config["openai"]["endpoint"],
                "body": body,
            }
            requests.append(json.dumps(request_line, ensure_ascii=False))
            request_map["requests"].append(
                {
                    "custom_id": custom_id,
                    "href": href,
                    "chunk_index": chunk_index,
                    "pairs": pairs,
                }
            )
    return requests, request_map, total_source_chars, total_translation_chars


def detect_output_text(body: Dict) -> str:
    parts = []
    for item in body.get("output", []):
        for content in item.get("content", []):
            text = content.get("text")
            if text:
                parts.append(text)
    if not parts and "output_text" in body:
        parts.append(body["output_text"])
    return "\n".join(parts).strip()


def post_openai_responses(api_key: str, body: Dict) -> Dict:
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if exc.code == 400 and '"param": "temperature"' in detail:
            raise RuntimeError(
                "OpenAI API rejected the 'temperature' parameter for this model. "
                "Disable it with configure-openai --no-send-temperature and retry."
            ) from exc
        raise RuntimeError(f"OpenAI API error: HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"OpenAI API request failed: {exc}") from exc


def get_openai_json(api_key: str, url: str) -> Dict:
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI API error: HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"OpenAI API request failed: {exc}") from exc


def download_openai_file(api_key: str, file_id: str, destination: Path) -> None:
    request = urllib.request.Request(
        f"https://api.openai.com/v1/files/{file_id}/content",
        method="GET",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            ensure_dir(destination.parent)
            destination.write_bytes(response.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI API error: HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"OpenAI file download failed: {exc}") from exc


def build_chunk_payload(
    chunk: List[TextUnit],
    glossary: Dict[str, str],
    target_language: str,
    qa_feedback_text: str = "",
    strong_formatting_retry: bool = False,
) -> List[Dict]:
    language_name = target_language_name(target_language)
    glossary_lines = [f"- {fr} => {en}" for fr, en in glossary.items()]
    glossary_text = "\n".join(glossary_lines) if glossary_lines else "- none yet"
    units = [{"id": index, "text": normalize_space(unit.text)} for index, unit in enumerate(chunk)]
    feedback_block = ""
    if qa_feedback_text:
        feedback_block = (
            "\nThis is a retry of a previously translated chunk.\n"
            "Use the QA findings below as corrective guidance while translating from the French source again.\n"
            "The old translation may be wrong; fix the underlying issues rather than paraphrasing the QA notes.\n"
            "If QA reported formatting or broken-syntax damage, reconstruct a fluent target sentence while still returning one translation per segment id.\n"
            f"{qa_feedback_text}\n"
        )
    formatting_retry_block = ""
    if strong_formatting_retry:
        formatting_retry_block = (
            "This chunk previously failed because segmentation damaged the syntax.\n"
            "Reconstruct the meaning across adjacent segments before deciding each segment-level wording.\n"
            "Do not mirror broken punctuation, dangling articles, or incomplete phrases from the previous attempt.\n"
            "Return translations that read naturally in sequence when the segments are concatenated in order.\n"
            "When a sentence spans multiple segments, coordinate the phrasing across those segments so the combined sentence is grammatical.\n"
        )
    user_prompt = (
        f"Translate the following French paragraph segments into {language_name}.\n"
        "Return strict JSON in the form {\"translations\": [{\"id\": 0, \"text\": \"...\"}]}.\n"
        "Do not omit any segment. Do not add commentary.\n"
        "Preserve the visible meaning, tone, and rhetorical structure.\n"
        f"Do not leave stray French words in the {language_name} unless they are proper nouns, publication titles, or intentionally retained foreign terms.\n"
        f"Avoid awkward literalism. Prefer idiomatic literary {language_name} where needed.\n"
        "Do not introduce doubled articles or doubled words.\n"
        "If you retain an italicized foreign term, inflect or frame the surrounding phrase so the sentence remains grammatically natural.\n"
        f"If a phrase is idiomatic or culturally embedded, render its meaning in fluent literary {language_name} rather than calquing it.\n"
        "Some segments contain inline-structure placeholders like [[SEG_1]], [[SEG_2]], etc.\n"
        "Copy every placeholder exactly once and keep them in ascending order inside that segment.\n"
        "Do not translate, remove, renumber, or duplicate placeholders.\n"
        f"{formatting_retry_block}"
        f"{feedback_block}"
        f"Glossary:\n{glossary_text}\n\n"
        f"Segments:\n{json.dumps(units, ensure_ascii=False)}"
    )
    return [
        {"role": "system", "content": build_system_prompt(target_language)},
        {"role": "user", "content": user_prompt},
    ]


def build_qa_payload(pairs: List[Dict[str, str]], target_language: str) -> List[Dict]:
    language_name = target_language_name(target_language)
    system_prompt = f"""You are a meticulous bilingual QA reviewer for book translations.
Compare French source text against its {language_name} translation.

Your job:
- identify only real problems,
- focus on meaning drift, omissions, leftover French, broken syntax, terminology inconsistency, and formatting-related text damage,
- ignore acceptable stylistic variation,
- keep the report concise,
- do not rewrite the whole passage,
- if there are no meaningful problems, return an empty issues list.
"""
    user_prompt = (
        f"Review the following French source segments and their {language_name} translations.\n"
        "Return strict JSON in the form "
        "{\"issues\": [{\"severity\": \"high|medium|low\", \"category\": \"accuracy|fluency|terminology|leftover_french|formatting\", "
        "\"source_excerpt\": \"...\", \"translation_excerpt\": \"...\", \"note\": \"...\"}], "
        "\"summary\": \"one short sentence\"}.\n"
        "Return at most 5 issues. Do not add commentary outside JSON.\n\n"
        f"Segments:\n{json.dumps(pairs, ensure_ascii=False)}"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def extract_translations_from_response(text: str, expected_count: Optional[int] = None) -> List[str]:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise RuntimeError("Could not find JSON object in model output.")
    data = json.loads(match.group(0))
    items = data.get("translations")
    if not isinstance(items, list):
        raise RuntimeError("Model output JSON is missing 'translations'.")
    by_id: Dict[int, str] = {}
    for item in items:
        item_id = item.get("id")
        item_text = item.get("text")
        if not isinstance(item_id, int) or not isinstance(item_text, str):
            continue
        by_id[item_id] = item_text
    if expected_count is not None:
        missing = [index for index in range(expected_count) if index not in by_id]
        if missing:
            raise RuntimeError(f"Model output JSON is missing segment ids: {missing[:5]}")
        return [by_id[index] for index in range(expected_count)]
    ordered = sorted(by_id.items())
    return [text for _, text in ordered]


def extract_json_object_from_response(text: str) -> Dict:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise RuntimeError("Could not find JSON object in model output.")
    return json.loads(match.group(0))


def normalize_translation_text(text: str) -> str:
    replacements = [
        ("the the ", "the "),
        ("The the ", "The "),
    ]
    normalized = text
    for old, new in replacements:
        normalized = normalized.replace(old, new)
    return normalized


def build_navigation_label_map(translated_dir: Path) -> Dict[str, str]:
    label_map: Dict[str, str] = {}

    def derive_label_from_file(path: Path) -> Optional[str]:
        try:
            tree = ET.parse(path)
        except ET.ParseError:
            return None
        root = tree.getroot()
        body = root.find(".//xhtml:body", NS)
        headings = []
        for tag in ("h1", "h2"):
            for elem in root.findall(f".//xhtml:{tag}", NS):
                text = normalize_space("".join(elem.itertext()))
                if text:
                    headings.append(text)
        if body is not None:
            paragraph_lines = []
            for elem in body.findall(".//xhtml:p", NS):
                text = normalize_space("".join(elem.itertext()))
                if text:
                    paragraph_lines.append(text)
                if len(paragraph_lines) >= 8:
                    break
            for index, line in enumerate(paragraph_lines):
                if re.fullmatch(r"CHAPTER\s+[IVXLCDM]+\.?", line, re.IGNORECASE):
                    if index + 1 < len(paragraph_lines):
                        next_line = paragraph_lines[index + 1]
                        next_line = re.sub(r"\s+\d+$", "", next_line).strip()
                        return f"{line.rstrip('.')} - {next_line}"
                    return line.rstrip(".")
                if line.upper() in {"PROLOGUE", "EPIGRAPH", "FOREWORD", "TABLE OF CONTENTS", "CONTENTS"}:
                    return line.title() if line.isupper() else line
        if headings:
            if len(headings) >= 2:
                first, second = headings[0], headings[1]
                if re.fullmatch(r"[IVXLCDM]+", first):
                    return f"{first} - {second}"
                if first.upper().startswith("PART "):
                    return f"{first} - {second}"
            return headings[0]
        anchors = root.findall(".//xhtml:a", NS)
        for anchor in anchors:
            text = normalize_space("".join(anchor.itertext()))
            if text:
                return text
        return None

    for path in sorted(translated_dir.rglob("*.xhtml")):
        rel = path.relative_to(translated_dir).as_posix()
        label = derive_label_from_file(path)
        if label:
            label_map[rel] = label

    candidates = sorted(
        [
            path for path in translated_dir.rglob("*.xhtml")
            if "toc" in path.name.lower()
        ]
    )
    for path in candidates:
        try:
            tree = ET.parse(path)
        except ET.ParseError:
            continue
        root = tree.getroot()
        for anchor in root.findall(".//xhtml:a", NS):
            href = anchor.attrib.get("href")
            text = normalize_space("".join(anchor.itertext()))
            if href and text and href not in label_map:
                label_map[href] = text
    return label_map


def normalize_translated_package(translated_dir: Path, target_language: str) -> None:
    normalized_lang = (target_language or "en").split("-", 1)[0].lower()
    html_lang = target_language or normalized_lang
    label_map = build_navigation_label_map(translated_dir)

    for html_path in list(translated_dir.rglob("*.xhtml")) + list(translated_dir.rglob("*.html")):
        try:
            tree = ET.parse(html_path)
        except ET.ParseError:
            continue
        root = tree.getroot()
        if strip_ns(root.tag) != "html":
            continue
        root.attrib["{http://www.w3.org/XML/1998/namespace}lang"] = html_lang
        root.attrib["lang"] = html_lang

        derived_label = label_map.get(html_path.relative_to(translated_dir).as_posix())
        head = root.find(".//xhtml:head", NS)
        if head is not None:
            title = head.find("xhtml:title", NS)
            if title is not None:
                title_text = normalize_space("".join(title.itertext()))
                if derived_label and title_text in {"Converted Ebook", "", "Cover"}:
                    title.text = derived_label
        tree.write(html_path, encoding="utf-8", xml_declaration=True)

    for opf_path in translated_dir.rglob("*.opf"):
        try:
            tree = ET.parse(opf_path)
        except ET.ParseError:
            continue
        root = tree.getroot()
        changed = False
        for lang in root.findall(".//{*}language"):
            if (lang.text or "").strip().lower() != normalized_lang:
                lang.text = normalized_lang
                changed = True
        if changed:
            tree.write(opf_path, encoding="utf-8", xml_declaration=True)

    for ncx_path in translated_dir.rglob("toc.ncx"):
        try:
            tree = ET.parse(ncx_path)
        except ET.ParseError:
            continue
        root = tree.getroot()
        root.attrib["{http://www.w3.org/XML/1998/namespace}lang"] = html_lang
        tree.write(ncx_path, encoding="utf-8", xml_declaration=True)


def sync_navigation_documents(translated_dir: Path) -> None:
    label_map = build_navigation_label_map(translated_dir)
    if not label_map:
        return

    for toc_path in translated_dir.rglob("toc.xhtml"):
        try:
            tree = ET.parse(toc_path)
        except ET.ParseError:
            continue
        root = tree.getroot()
        changed = False
        for anchor in root.findall(".//xhtml:a", NS):
            href = anchor.attrib.get("href")
            if href in label_map:
                anchor.text = label_map[href]
                changed = True
        if changed:
            tree.write(toc_path, encoding="utf-8", xml_declaration=True)

    for ncx_path in translated_dir.rglob("toc.ncx"):
        try:
            tree = ET.parse(ncx_path)
        except ET.ParseError:
            continue
        root = tree.getroot()
        changed = False
        for nav_point in root.findall(".//{*}navPoint"):
            content = nav_point.find("{*}content")
            label = nav_point.find("{*}navLabel/{*}text")
            if content is None or label is None:
                continue
            src = content.attrib.get("src")
            if src in label_map:
                label.text = label_map[src]
                changed = True
        if changed:
            tree.write(ncx_path, encoding="utf-8", xml_declaration=True)


def text_unit_from_dict(data: Dict) -> TextUnit:
    return TextUnit(
        xpath=data["xpath"],
        field=data["field"],
        text=data["text"],
        plain_text=data.get("plain_text") or normalize_space("".join(target["original_text"] for target in data["targets"])),
        targets=[TextTarget(**target) for target in data["targets"]],
    )


def infer_project_dir(project: Optional[str], epub: Optional[str], project_root: Path, target_language: Optional[str] = None) -> Path:
    if project:
        project_path = Path(project)
        return project_path if project_path.is_absolute() else project_root / project_path
    if not epub:
        raise RuntimeError("Provide either --project or --epub.")
    slug = slugify(Path(epub).stem)
    normalized_lang = (target_language or "").split("-", 1)[0].lower()
    if normalized_lang and normalized_lang != "en":
        slug = f"{slug}-{normalized_lang}"
    return project_root / slug


def create_progress_stub(epub_path: Path) -> Dict:
    return {"epub": str(epub_path.resolve()), "completed": {}, "failed": {}, "translations": {}}


def cmd_init_project(args: argparse.Namespace) -> None:
    project_root = Path(args.project_root)
    epub_path = Path(args.epub).resolve()
    target_language = args.target_language or "en"
    project_dir = infer_project_dir(args.project, args.epub, project_root, target_language=target_language)
    if project_dir.exists() and any(project_dir.iterdir()) and not args.force:
        raise RuntimeError(f"Project directory already exists: {project_dir}")
    ensure_dir(project_dir)
    paths = project_paths(project_dir, target_language=target_language)
    for key in ("project_dir", "source_dir", "translated_dir", "batch_requests", "batch_map"):
        if isinstance(paths[key], Path):
            ensure_dir(paths[key].parent if paths[key].suffix else paths[key])
    config = default_project_config(epub_path, project_dir, target_language=target_language)
    save_project_config(project_dir, config)
    write_default_glossary(paths["glossary"], target_language=target_language)
    write_default_qa(paths["qa"])
    save_json(paths["progress"], create_progress_stub(epub_path))
    unzip_epub(epub_path, paths["source_dir"])
    shutil.copytree(paths["source_dir"], paths["translated_dir"], dirs_exist_ok=True)
    print(project_dir)


def cmd_list_content(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, args.epub, Path(args.project_root))
    config = read_project_config(project_dir)
    source_dir = project_paths(project_dir)["source_dir"]
    if not source_dir.exists():
        unzip_epub(Path(config["book"]["source_epub"]), source_dir)
    for href in visible_content_files(source_dir):
        _, units = collect_text_units(source_dir / href)
        text = normalize_space(" ".join(unit.plain_text for unit in units))
        print(f"{href}\twords={len(text.split())}\tchars={len(text)}")


def cmd_configure_openai(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    openai = config["openai"]
    if args.model is not None:
        openai["model"] = args.model
    if args.temperature is not None:
        openai["temperature"] = args.temperature
    if args.send_temperature is not None:
        openai["send_temperature"] = args.send_temperature
    if args.reasoning_effort is not None:
        openai["reasoning_effort"] = None if args.reasoning_effort == "none" else args.reasoning_effort
    if args.use_batch is not None:
        openai["use_batch_api"] = args.use_batch
    if args.run_qa_after_apply is not None:
        config["translation"]["run_qa_after_apply"] = args.run_qa_after_apply
    save_project_config(project_dir, config)
    print(json.dumps({"openai": openai, "translation": config["translation"]}, ensure_ascii=False, indent=2))


def cmd_estimate_cost(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    model = args.model or config["openai"].get("model")
    if not model:
        raise RuntimeError("No model configured. Use configure-openai first.")
    pricing = PRICE_TABLE.get(model)
    if not pricing:
        raise RuntimeError(f"No local price table for model '{model}'. Add it before estimating cost.")
    source_dir = project_paths(project_dir)["source_dir"]
    total_chars = 0
    total_words = 0
    files = visible_content_files(source_dir)
    for href in files:
        _, units = collect_text_units(source_dir / href)
        text = normalize_space(" ".join(unit.plain_text for unit in units))
        total_chars += len(text)
        total_words += len(text.split())
    input_tokens = math.ceil(total_chars / 4)
    output_tokens = math.ceil(input_tokens * 1.02)
    if config["openai"].get("use_batch_api", True):
        discount = 0.5
    else:
        discount = 1.0
    input_cost = input_tokens / 1_000_000 * pricing["input"] * discount
    output_cost = output_tokens / 1_000_000 * pricing["output"] * discount
    report = {
        "model": model,
        "batch_discount_applied": discount == 0.5,
        "estimated_words": total_words,
        "estimated_input_tokens": input_tokens,
        "estimated_output_tokens": output_tokens,
        "estimated_total_usd": round(input_cost + output_cost, 4),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


def cmd_suggest_glossary(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    paths = project_paths(project_dir)
    source_dir = paths["source_dir"]
    if not source_dir.exists():
        unzip_epub(Path(config["book"]["source_epub"]), source_dir)
    files = [args.chapter] if args.chapter else visible_content_files(source_dir)
    suggestions = suggest_glossary_candidates(
        source_dir,
        files,
        load_glossary(paths["glossary"]),
        max_candidates=args.max_candidates,
    )
    write_glossary_suggestions(
        paths["glossary_suggestions"],
        project_dir.name,
        config["translation"].get("target_language", "en"),
        suggestions,
    )
    print(
        json.dumps(
            {
                "suggestions": len(suggestions),
                "glossary_suggestions": str(paths["glossary_suggestions"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_prepare_batch(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    model = config["openai"].get("model")
    if not model:
        raise RuntimeError("Set a model first with configure-openai.")
    paths = project_paths(project_dir)
    source_dir = paths["source_dir"]
    glossary = load_glossary(paths["glossary"])
    qa_feedback = load_qa_feedback(paths["qa_cloud"])
    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))
    selected_files = [args.chapter] if args.chapter else visible_content_files(source_dir)

    requests: List[str] = []
    request_map = {"project": project_dir.name, "requests": []}
    max_chars = args.max_chars or config["translation"]["max_chars_per_chunk"]
    retry_only_high = args.retry_only_high
    for href in selected_files:
        _, units = collect_text_units(source_dir / href)
        chunks = chunk_units(units, max_chars)
        completed = set(progress["completed"].get(href, []))
        qa_feedback_for_file = qa_feedback.get(href, {})
        for chunk_index, chunk in enumerate(chunks):
            if not should_translate_chunk(chunk_index, completed, qa_feedback_for_file, retry_only_high=retry_only_high):
                continue
            chunk_feedback = qa_feedback_for_file.get(chunk_index)
            qa_feedback_text = build_qa_feedback_text(chunk_feedback)
            strong_formatting_retry = chunk_has_high_formatting_issue(chunk_feedback)
            custom_id = f"{project_dir.name}:{href}:chunk:{chunk_index:04d}"
            body = {
                "model": model,
                "input": build_chunk_payload(
                    chunk,
                    glossary,
                    config["translation"].get("target_language", "en"),
                    qa_feedback_text=qa_feedback_text,
                    strong_formatting_retry=strong_formatting_retry,
                ),
            }
            temperature = config["openai"].get("temperature")
            if config["openai"].get("send_temperature", True) and temperature is not None:
                body["temperature"] = temperature
            reasoning_effort = config["openai"].get("reasoning_effort")
            if reasoning_effort:
                body["reasoning"] = {"effort": reasoning_effort}
            request_line = {
                "custom_id": custom_id,
                "method": "POST",
                "url": config["openai"]["endpoint"],
                "body": body,
            }
            requests.append(json.dumps(request_line, ensure_ascii=False))
            request_map["requests"].append(
                {
                    "custom_id": custom_id,
                    "href": href,
                    "chunk_index": chunk_index,
                    "qa_feedback_used": bool(qa_feedback_text),
                    "strong_formatting_retry": strong_formatting_retry,
                    "units": [asdict(unit) for unit in chunk],
                }
            )
    ensure_dir(paths["batch_requests"].parent)
    paths["batch_requests"].write_text("\n".join(requests) + ("\n" if requests else ""), encoding="utf-8")
    save_json(paths["batch_map"], request_map)
    print(json.dumps({"requests": len(requests), "batch_requests": str(paths["batch_requests"])}, ensure_ascii=False, indent=2))


def cmd_estimate_qa_cost(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    model = args.model or config["openai"].get("model")
    if model not in PRICE_TABLE:
        raise RuntimeError(f"No pricing known for model '{model}'.")
    paths = project_paths(project_dir)
    selected_files = [args.chapter] if args.chapter else visible_content_files(paths["source_dir"])
    max_chars = args.max_chars or config["translation"]["max_chars_per_chunk"]
    requests, _, total_source_chars, total_translation_chars = build_qa_requests(project_dir, config, selected_files, max_chars)
    chunk_count = len(requests)
    # Lightweight QA prompt: two texts + compact issue list response.
    input_tokens = math.ceil((total_source_chars + total_translation_chars) / 4) + chunk_count * 420
    output_tokens = chunk_count * 140
    pricing = PRICE_TABLE[model]
    input_cost = input_tokens / 1_000_000 * pricing["input"] * 0.5
    output_cost = output_tokens / 1_000_000 * pricing["output"] * 0.5
    report = {
        "model": model,
        "batch_discount_applied": True,
        "qa_chunks": chunk_count,
        "estimated_input_tokens": input_tokens,
        "estimated_output_tokens": output_tokens,
        "estimated_total_usd": round(input_cost + output_cost, 4),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


def cmd_prepare_qa_batch(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    paths = project_paths(project_dir)
    selected_files = [args.chapter] if args.chapter else visible_content_files(paths["source_dir"])
    max_chars = args.max_chars or config["translation"]["max_chars_per_chunk"]
    requests, request_map, _, _ = build_qa_requests(project_dir, config, selected_files, max_chars)
    ensure_dir(paths["qa_batch_requests"].parent)
    paths["qa_batch_requests"].write_text("\n".join(requests) + ("\n" if requests else ""), encoding="utf-8")
    save_json(paths["qa_batch_map"], request_map)
    print(json.dumps({"requests": len(requests), "qa_batch_requests": str(paths["qa_batch_requests"])}, ensure_ascii=False, indent=2))


def cmd_run_batch(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    config = read_project_config(project_dir)
    model = config["openai"].get("model")
    if not model:
        raise RuntimeError("Set a model first with configure-openai.")
    paths = project_paths(project_dir)
    source_dir = paths["source_dir"]
    glossary = load_glossary(paths["glossary"])
    qa_feedback = load_qa_feedback(paths["qa_cloud"])
    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))
    selected_files = [args.chapter] if args.chapter else visible_content_files(source_dir)
    max_chars = args.max_chars or config["translation"]["max_chars_per_chunk"]

    requests: List[str] = []
    request_map = {"project": project_dir.name, "requests": []}
    qa_retry_chunks = 0
    strong_formatting_retries = 0
    retry_only_high = args.retry_only_high
    for href in selected_files:
        _, units = collect_text_units(source_dir / href)
        chunks = chunk_units(units, max_chars)
        completed = set(progress["completed"].get(href, []))
        qa_feedback_for_file = qa_feedback.get(href, {})
        for chunk_index, chunk in enumerate(chunks):
            if not should_translate_chunk(chunk_index, completed, qa_feedback_for_file, retry_only_high=retry_only_high):
                continue
            chunk_feedback = qa_feedback_for_file.get(chunk_index)
            qa_feedback_text = build_qa_feedback_text(chunk_feedback)
            strong_formatting_retry = chunk_has_high_formatting_issue(chunk_feedback)
            if qa_feedback_text:
                qa_retry_chunks += 1
            if strong_formatting_retry:
                strong_formatting_retries += 1
            custom_id = f"{project_dir.name}:{href}:chunk:{chunk_index:04d}"
            body = {
                "model": model,
                "input": build_chunk_payload(
                    chunk,
                    glossary,
                    config["translation"].get("target_language", "en"),
                    qa_feedback_text=qa_feedback_text,
                    strong_formatting_retry=strong_formatting_retry,
                ),
            }
            temperature = config["openai"].get("temperature")
            if config["openai"].get("send_temperature", True) and temperature is not None:
                body["temperature"] = temperature
            reasoning_effort = config["openai"].get("reasoning_effort")
            if reasoning_effort:
                body["reasoning"] = {"effort": reasoning_effort}
            request_line = {
                "custom_id": custom_id,
                "method": "POST",
                "url": config["openai"]["endpoint"],
                "body": body,
            }
            requests.append(json.dumps(request_line, ensure_ascii=False))
            request_map["requests"].append(
                {
                    "custom_id": custom_id,
                    "href": href,
                    "chunk_index": chunk_index,
                    "qa_feedback_used": bool(qa_feedback_text),
                    "strong_formatting_retry": strong_formatting_retry,
                    "units": [asdict(unit) for unit in chunk],
                }
            )

    ensure_dir(paths["batch_requests"].parent)
    paths["batch_requests"].write_text("\n".join(requests) + ("\n" if requests else ""), encoding="utf-8")
    save_json(paths["batch_map"], request_map)
    log(
        f"[run-batch] Prepared {len(requests)} request(s) at {paths['batch_requests']} "
        f"(qa_retry_chunks={qa_retry_chunks}, strong_formatting_retries={strong_formatting_retries})"
    )

    input_file_id = upload_batch_file(api_key, paths["batch_requests"])
    log(f"[run-batch] Uploaded batch input file: {input_file_id}")
    payload = json.dumps(
        {
            "input_file_id": input_file_id,
            "endpoint": config["openai"]["endpoint"],
            "completion_window": config["openai"]["completion_window"],
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        "https://api.openai.com/v1/batches",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Batch submission failed: {exc}") from exc
    state_path = project_dir / "batches" / "last_batch.json"
    save_json(state_path, data)
    print(
        json.dumps(
            {
                "requests": len(requests),
                "batch_requests": str(paths["batch_requests"]),
                "batch_id": data.get("id"),
                "status": data.get("status"),
                "input_file_id": input_file_id,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_run_qa_batch(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    config = read_project_config(project_dir)
    paths = project_paths(project_dir)
    selected_files = [args.chapter] if args.chapter else visible_content_files(paths["source_dir"])
    max_chars = args.max_chars or config["translation"]["max_chars_per_chunk"]
    requests, request_map, _, _ = build_qa_requests(project_dir, config, selected_files, max_chars)

    ensure_dir(paths["qa_batch_requests"].parent)
    paths["qa_batch_requests"].write_text("\n".join(requests) + ("\n" if requests else ""), encoding="utf-8")
    save_json(paths["qa_batch_map"], request_map)
    log(f"[run-qa-batch] Prepared {len(requests)} request(s) at {paths['qa_batch_requests']}")

    input_file_id = upload_batch_file(api_key, paths["qa_batch_requests"])
    log(f"[run-qa-batch] Uploaded batch input file: {input_file_id}")
    payload = json.dumps(
        {
            "input_file_id": input_file_id,
            "endpoint": config["openai"]["endpoint"],
            "completion_window": config["openai"]["completion_window"],
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        "https://api.openai.com/v1/batches",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Batch submission failed: {exc}") from exc
    save_json(paths["qa_last_batch"], data)
    print(
        json.dumps(
            {
                "requests": len(requests),
                "qa_batch_requests": str(paths["qa_batch_requests"]),
                "batch_id": data.get("id"),
                "status": data.get("status"),
                "input_file_id": input_file_id,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_translate_direct(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    model = config["openai"].get("model")
    if not model:
        raise RuntimeError("Set a model first with configure-openai.")

    paths = project_paths(project_dir)
    source_dir = paths["source_dir"]
    glossary = load_glossary(paths["glossary"])
    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))

    chapter_list = [args.chapter] if args.chapter else visible_content_files(source_dir)
    qa_feedback = load_qa_feedback(paths["qa_cloud"])
    retry_only_high = args.retry_only_high
    remaining_budget = args.max_chunks
    translated_summary = []
    total_selected = 0
    for href in chapter_list:
        source_file = source_dir / href
        if not source_file.exists():
            continue
        _, units = collect_text_units(source_file)
        chunks = chunk_units(units, args.max_chars or config["translation"]["max_chars_per_chunk"])
        completed_set = set(progress["completed"].get(href, []))
        qa_feedback_for_file = qa_feedback.get(href, {})
        total_selected += len(
            [
                idx
                for idx in range(len(chunks))
                if should_translate_chunk(idx, completed_set, qa_feedback_for_file, retry_only_high=retry_only_high)
            ]
        )
    log(f"[translate-direct] Starting. Chunks selected in scope: {total_selected}")
    processed_count = 0

    for href in chapter_list:
        if remaining_budget is not None and remaining_budget <= 0:
            break
        source_file = source_dir / href
        if not source_file.exists():
            raise RuntimeError(f"Chapter file not found: {source_file}")

        tree, units = collect_text_units(source_file)
        chunks = chunk_units(units, args.max_chars or config["translation"]["max_chars_per_chunk"])
        completed_set = set(progress["completed"].get(href, []))
        qa_feedback_for_file = qa_feedback.get(href, {})
        available_indexes = [
            idx
            for idx in range(len(chunks))
            if should_translate_chunk(idx, completed_set, qa_feedback_for_file, retry_only_high=retry_only_high)
        ]
        if not available_indexes:
            continue
        qa_retry_count = len([idx for idx in available_indexes if idx in qa_feedback_for_file])
        log(
            f"[translate-direct] Chapter {href}: selected chunks {len(available_indexes)} / total {len(chunks)} "
            f"(qa_retry_chunks={qa_retry_count})"
        )
        if remaining_budget is None:
            selected_indexes = available_indexes
        else:
            selected_indexes = available_indexes[:remaining_budget]
        translated_map = {
            tuple(key.split("::", 1)): value
            for key, value in progress["translations"].get(href, {}).items()
        }
        translated_chunks = []
        for chunk_index in selected_indexes:
            chunk = chunks[chunk_index]
            log(
                f"[translate-direct] Sending {href} chunk {chunk_index + 1}/{len(chunks)} "
                f"({len(chunk)} segments, approx_chars={sum(len(u.plain_text) for u in chunk)})"
            )
            chunk_feedback = qa_feedback_for_file.get(chunk_index)
            qa_feedback_text = build_qa_feedback_text(chunk_feedback)
            strong_formatting_retry = chunk_has_high_formatting_issue(chunk_feedback)
            body = {
                "model": model,
                "input": build_chunk_payload(
                    chunk,
                    glossary,
                    config["translation"].get("target_language", "en"),
                    qa_feedback_text=qa_feedback_text,
                    strong_formatting_retry=strong_formatting_retry,
                ),
            }
            temperature = config["openai"].get("temperature")
            if config["openai"].get("send_temperature", True) and temperature is not None:
                body["temperature"] = temperature
            reasoning_effort = config["openai"].get("reasoning_effort")
            if reasoning_effort:
                body["reasoning"] = {"effort": reasoning_effort}
            response = post_openai_responses(api_key, body)
            log(f"[translate-direct] Received response {response.get('id')} for {href} chunk {chunk_index + 1}/{len(chunks)}")
            output_text = detect_output_text(response)
            translations = extract_translations_from_response(output_text, expected_count=len(chunk))
            if len(translations) != len(chunk):
                raise RuntimeError(f"Segment count mismatch in chapter {href}, chunk {chunk_index}.")
            for unit, translated_text in zip(chunk, translations):
                split_parts = split_translated_text(unit, normalize_translation_text(translated_text))
                for target, piece in zip(unit.targets, split_parts):
                    translated_map[(target.xpath, target.field)] = piece
            progress["translations"][href] = {
                f"{xpath}::{field}": value for (xpath, field), value in translated_map.items()
            }
            progress["completed"].setdefault(href, [])
            if chunk_index not in progress["completed"][href]:
                progress["completed"][href].append(chunk_index)
            translated_chunks.append(
                {
                    "chunk_index": chunk_index,
                    "segment_count": len(chunk),
                    "response_id": response.get("id"),
                }
            )
            save_json(paths["progress"], progress)
            processed_count += 1
            log(f"[translate-direct] Saved progress. Completed {processed_count} chunk(s) this run.")
            if remaining_budget is not None:
                remaining_budget -= 1
                if remaining_budget <= 0:
                    break

        assign_translations(tree, translated_map)
        target_file = paths["translated_dir"] / href
        ensure_dir(target_file.parent)
        tree.write(target_file, encoding="utf-8", xml_declaration=True)
        if translated_chunks:
            translated_summary.append({"chapter": href, "translated_chunks": translated_chunks})

    normalize_translated_package(paths["translated_dir"], config["translation"].get("target_language", "en"))
    sync_navigation_documents(paths["translated_dir"])
    rezip_epub(paths["translated_dir"], paths["output_epub"])

    result = {
        "chapters_updated": translated_summary,
        "output_epub": str(paths["output_epub"]),
        "qa_ran": False,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


def upload_batch_file(api_key: str, batch_requests: Path) -> str:
    boundary = "----codexbatchboundary"
    file_bytes = batch_requests.read_bytes()
    payload = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="purpose"\r\n\r\n'
        "batch\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{batch_requests.name}"\r\n'
        "Content-Type: application/jsonl\r\n\r\n"
    ).encode("utf-8") + file_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")
    request = urllib.request.Request(
        "https://api.openai.com/v1/files",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        data = json.loads(response.read().decode("utf-8"))
    return data["id"]


def cmd_submit_batch(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    paths = project_paths(project_dir)
    input_file_id = upload_batch_file(api_key, paths["batch_requests"])
    payload = json.dumps(
        {
            "input_file_id": input_file_id,
            "endpoint": config["openai"]["endpoint"],
            "completion_window": config["openai"]["completion_window"],
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        "https://api.openai.com/v1/batches",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Batch submission failed: {exc}") from exc
    state_path = project_dir / "batches" / "last_batch.json"
    save_json(state_path, data)
    print(json.dumps(data, ensure_ascii=False, indent=2))


def load_batch_state(project_dir: Path, batch_id: Optional[str], qa: bool = False) -> Tuple[Path, Dict]:
    paths = project_paths(project_dir)
    state_path = paths["qa_last_batch"] if qa else project_dir / "batches" / "last_batch.json"
    if batch_id:
        return state_path, {"id": batch_id}
    return state_path, load_json(state_path, {})


def cmd_batch_status(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    state_path, state = load_batch_state(project_dir, args.batch_id, qa=False)
    batch_id = args.batch_id or state.get("id")
    if not batch_id:
        raise RuntimeError("No batch id available. Submit a batch first or pass --batch-id.")
    data = get_openai_json(api_key, f"https://api.openai.com/v1/batches/{batch_id}")
    save_json(state_path, data)
    print(json.dumps(data, ensure_ascii=False, indent=2))


def cmd_batch_download_output(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    paths = project_paths(project_dir)
    state_path, state = load_batch_state(project_dir, args.batch_id, qa=False)
    batch_id = args.batch_id or state.get("id")
    if not batch_id:
        raise RuntimeError("No batch id available. Submit a batch first or pass --batch-id.")
    if not state or state.get("id") != batch_id or not state.get("output_file_id"):
        state = get_openai_json(api_key, f"https://api.openai.com/v1/batches/{batch_id}")
        save_json(state_path, state)
    output_file_id = state.get("output_file_id")
    if not output_file_id:
        raise RuntimeError(f"Batch {batch_id} has no output_file_id yet. Check status again later.")
    download_openai_file(api_key, output_file_id, paths["batch_output"])
    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "output_file_id": output_file_id,
                "saved_to": str(paths["batch_output"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_qa_batch_status(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    state_path, state = load_batch_state(project_dir, args.batch_id, qa=True)
    batch_id = args.batch_id or state.get("id")
    if not batch_id:
        raise RuntimeError("No QA batch id available. Run run-qa-batch first or pass --batch-id.")
    data = get_openai_json(api_key, f"https://api.openai.com/v1/batches/{batch_id}")
    save_json(state_path, data)
    print(json.dumps(data, ensure_ascii=False, indent=2))


def cmd_qa_batch_download_output(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    paths = project_paths(project_dir)
    state_path, state = load_batch_state(project_dir, args.batch_id, qa=True)
    batch_id = args.batch_id or state.get("id")
    if not batch_id:
        raise RuntimeError("No QA batch id available. Run run-qa-batch first or pass --batch-id.")
    if not state or state.get("id") != batch_id or not state.get("output_file_id"):
        state = get_openai_json(api_key, f"https://api.openai.com/v1/batches/{batch_id}")
        save_json(state_path, state)
    output_file_id = state.get("output_file_id")
    if not output_file_id:
        raise RuntimeError(f"QA batch {batch_id} has no output_file_id yet. Check status again later.")
    download_openai_file(api_key, output_file_id, paths["qa_batch_output"])
    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "output_file_id": output_file_id,
                "saved_to": str(paths["qa_batch_output"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_apply_batch_output(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    paths = project_paths(project_dir)
    batch_map = load_json(paths["batch_map"], {"requests": []})
    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))
    output_path = Path(args.batch_output) if args.batch_output else paths["batch_output"]
    if not output_path.exists():
        raise RuntimeError(f"Batch output file not found: {output_path}")
    outputs = {}
    for line in output_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        outputs[item["custom_id"]] = item

    changed_files: List[Path] = []
    for request in batch_map["requests"]:
        custom_id = request["custom_id"]
        if custom_id not in outputs:
            continue
        response = outputs[custom_id]
        if response.get("error"):
            progress["failed"][custom_id] = response["error"]
            continue
        text = detect_output_text(response["response"]["body"])
        units = [text_unit_from_dict(unit) for unit in request["units"]]
        translations = extract_translations_from_response(text, expected_count=len(units))
        href = request["href"]
        translated_map = {
            tuple(key.split("::", 1)): value
            for key, value in progress["translations"].get(href, {}).items()
        }
        for unit, translated_text in zip(units, translations):
            split_parts = split_translated_text(unit, normalize_translation_text(translated_text))
            for target, piece in zip(unit.targets, split_parts):
                translated_map[(target.xpath, target.field)] = piece
        progress["translations"][href] = {
            f"{xpath}::{field}": value for (xpath, field), value in translated_map.items()
        }
        progress["completed"].setdefault(href, [])
        if request["chunk_index"] not in progress["completed"][href]:
            progress["completed"][href].append(request["chunk_index"])
        source_file = paths["source_dir"] / href
        target_file = paths["translated_dir"] / href
        tree, _ = collect_text_units(source_file)
        assign_translations(tree, translated_map)
        ensure_dir(target_file.parent)
        tree.write(target_file, encoding="utf-8", xml_declaration=True)
        changed_files.append(target_file)
    save_json(paths["progress"], progress)
    normalize_translated_package(paths["translated_dir"], config["translation"].get("target_language", "en"))
    sync_navigation_documents(paths["translated_dir"])
    rezip_epub(paths["translated_dir"], paths["output_epub"])
    run_qa = config["translation"].get("run_qa_after_apply", True)
    if args.skip_qa:
        run_qa = False
    if run_qa:
        generate_qa_report(changed_files, load_glossary(paths["glossary"]), paths["qa"])
    print(
        json.dumps(
            {
                "updated_files": len(changed_files),
                "output_epub": str(paths["output_epub"]),
                "qa_ran": run_qa,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_apply_qa_output(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    paths = project_paths(project_dir)
    batch_map = load_json(paths["qa_batch_map"], {"requests": []})
    output_path = Path(args.batch_output) if args.batch_output else paths["qa_batch_output"]
    if not output_path.exists():
        raise RuntimeError(f"QA batch output file not found: {output_path}")

    outputs = {}
    for line in output_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        outputs[item["custom_id"]] = item

    report_lines = ["# Cloud QA", "", f"Project: `{project_dir.name}`", "", "## Findings", ""]
    finding_count = 0
    checked_chunks = 0
    failed_chunks = 0

    for request in batch_map["requests"]:
        custom_id = request["custom_id"]
        if custom_id not in outputs:
            continue
        checked_chunks += 1
        response = outputs[custom_id]
        if response.get("error"):
            failed_chunks += 1
            report_lines.append(f"### {request['href']} chunk {request['chunk_index']}")
            report_lines.append(f"- error: `{response['error']}`")
            report_lines.append("")
            continue
        text = detect_output_text(response["response"]["body"])
        try:
            data = extract_json_object_from_response(text)
        except Exception as exc:
            failed_chunks += 1
            report_lines.append(f"### {request['href']} chunk {request['chunk_index']}")
            report_lines.append(f"- error: `Could not parse QA JSON: {exc}`")
            report_lines.append("")
            continue
        issues = data.get("issues") or []
        summary = data.get("summary") or "No summary."
        if not issues:
            continue
        report_lines.append(f"### {request['href']} chunk {request['chunk_index']}")
        report_lines.append(f"- summary: {summary}")
        for issue in issues:
            severity = issue.get("severity", "unknown")
            category = issue.get("category", "unknown")
            source_excerpt = normalize_space(issue.get("source_excerpt", ""))
            translation_excerpt = normalize_space(issue.get("translation_excerpt", ""))
            note = normalize_space(issue.get("note", ""))
            report_lines.append(f"- `{severity}` `{category}`: {note}")
            if source_excerpt:
                report_lines.append(f"- source: {source_excerpt}")
            if translation_excerpt:
                report_lines.append(f"- translation: {translation_excerpt}")
            finding_count += 1
        report_lines.append("")

    if finding_count == 0:
        report_lines.append("- No meaningful issues were reported by the cloud QA run.")
        report_lines.append("")
    report_lines.append("## Summary")
    report_lines.append(f"- checked_chunks: `{checked_chunks}`")
    report_lines.append(f"- findings: `{finding_count}`")
    report_lines.append(f"- failed_chunks: `{failed_chunks}`")
    report_text = "\n".join(report_lines)
    paths["qa_cloud"].write_text(report_text, encoding="utf-8")
    ensure_dir(paths["qa_cloud_history"])
    history_filename = build_qa_history_filename(batch_map.get("requests", []))
    history_path = unique_path(paths["qa_cloud_history"] / history_filename)
    history_path.write_text(report_text, encoding="utf-8")
    print(
        json.dumps(
            {
                "checked_chunks": checked_chunks,
                "findings": finding_count,
                "failed_chunks": failed_chunks,
                "qa_report": str(paths["qa_cloud"]),
                "qa_history_snapshot": str(history_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="EPUB translation pipeline with per-book projects and OpenAI batch support.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-project", help="Create a per-book project workspace.")
    init_parser.add_argument("--epub", required=True)
    init_parser.add_argument("--project")
    init_parser.add_argument("--target-language", default="en")
    init_parser.add_argument("--project-root", default="projects")
    init_parser.add_argument("--force", action="store_true")
    init_parser.set_defaults(func=cmd_init_project)

    list_parser = subparsers.add_parser("list-content", help="List XHTML/HTML content files for a project.")
    list_parser.add_argument("--project")
    list_parser.add_argument("--epub")
    list_parser.add_argument("--project-root", default="projects")
    list_parser.set_defaults(func=cmd_list_content)

    configure_parser = subparsers.add_parser("configure-openai", help="Set the model and OpenAI translation parameters.")
    configure_parser.add_argument("--project", required=True)
    configure_parser.add_argument("--project-root", default="projects")
    configure_parser.add_argument("--model")
    configure_parser.add_argument("--temperature", type=float)
    configure_parser.add_argument("--send-temperature", action=argparse.BooleanOptionalAction, default=None)
    configure_parser.add_argument("--reasoning-effort", choices=["none", "low", "medium", "high"])
    configure_parser.add_argument("--use-batch", action=argparse.BooleanOptionalAction, default=None)
    configure_parser.add_argument("--run-qa-after-apply", action=argparse.BooleanOptionalAction, default=None)
    configure_parser.set_defaults(func=cmd_configure_openai)

    estimate_parser = subparsers.add_parser("estimate-cost", help="Estimate translation cost for a configured project.")
    estimate_parser.add_argument("--project", required=True)
    estimate_parser.add_argument("--project-root", default="projects")
    estimate_parser.add_argument("--model")
    estimate_parser.set_defaults(func=cmd_estimate_cost)

    glossary_parser = subparsers.add_parser("suggest-glossary", help="Extract glossary candidates from the source EPUB into a per-project suggestions file.")
    glossary_parser.add_argument("--project", required=True)
    glossary_parser.add_argument("--project-root", default="projects")
    glossary_parser.add_argument("--chapter")
    glossary_parser.add_argument("--max-candidates", type=int, default=80)
    glossary_parser.set_defaults(func=cmd_suggest_glossary)

    qa_estimate_parser = subparsers.add_parser("estimate-qa-cost", help="Estimate cloud QA cost for a configured translated project.")
    qa_estimate_parser.add_argument("--project", required=True)
    qa_estimate_parser.add_argument("--project-root", default="projects")
    qa_estimate_parser.add_argument("--model")
    qa_estimate_parser.add_argument("--chapter")
    qa_estimate_parser.add_argument("--max-chars", type=int)
    qa_estimate_parser.set_defaults(func=cmd_estimate_qa_cost)

    prepare_parser = subparsers.add_parser("prepare-batch", help="Generate JSONL requests for OpenAI Batch API.")
    prepare_parser.add_argument("--project", required=True)
    prepare_parser.add_argument("--project-root", default="projects")
    prepare_parser.add_argument("--chapter")
    prepare_parser.add_argument("--max-chars", type=int)
    prepare_parser.add_argument("--retry-only-high", action="store_true", help="When reusing QA_cloud feedback, retry only chunks with at least one high-severity QA issue.")
    prepare_parser.set_defaults(func=cmd_prepare_batch)

    qa_prepare_parser = subparsers.add_parser("prepare-qa-batch", help="Generate JSONL requests for OpenAI Batch QA.")
    qa_prepare_parser.add_argument("--project", required=True)
    qa_prepare_parser.add_argument("--project-root", default="projects")
    qa_prepare_parser.add_argument("--chapter")
    qa_prepare_parser.add_argument("--max-chars", type=int)
    qa_prepare_parser.set_defaults(func=cmd_prepare_qa_batch)

    run_batch_parser = subparsers.add_parser("run-batch", help="Prepare and submit a batch in one command for all remaining chunks or one chapter.")
    run_batch_parser.add_argument("--project", required=True)
    run_batch_parser.add_argument("--project-root", default="projects")
    run_batch_parser.add_argument("--chapter")
    run_batch_parser.add_argument("--max-chars", type=int)
    run_batch_parser.add_argument("--retry-only-high", action="store_true", help="When reusing QA_cloud feedback, retry only chunks with at least one high-severity QA issue.")
    run_batch_parser.set_defaults(func=cmd_run_batch)

    run_qa_batch_parser = subparsers.add_parser("run-qa-batch", help="Prepare and submit a cloud QA batch for all translated chunks or one chapter.")
    run_qa_batch_parser.add_argument("--project", required=True)
    run_qa_batch_parser.add_argument("--project-root", default="projects")
    run_qa_batch_parser.add_argument("--chapter")
    run_qa_batch_parser.add_argument("--max-chars", type=int)
    run_qa_batch_parser.set_defaults(func=cmd_run_qa_batch)

    submit_parser = subparsers.add_parser("submit-batch", help="Submit the prepared batch to OpenAI.")
    submit_parser.add_argument("--project", required=True)
    submit_parser.add_argument("--project-root", default="projects")
    submit_parser.set_defaults(func=cmd_submit_batch)

    batch_status_parser = subparsers.add_parser("batch-status", help="Fetch the current status of the latest or selected batch.")
    batch_status_parser.add_argument("--project", required=True)
    batch_status_parser.add_argument("--project-root", default="projects")
    batch_status_parser.add_argument("--batch-id")
    batch_status_parser.set_defaults(func=cmd_batch_status)

    qa_batch_status_parser = subparsers.add_parser("qa-batch-status", help="Fetch the current status of the latest or selected QA batch.")
    qa_batch_status_parser.add_argument("--project", required=True)
    qa_batch_status_parser.add_argument("--project-root", default="projects")
    qa_batch_status_parser.add_argument("--batch-id")
    qa_batch_status_parser.set_defaults(func=cmd_qa_batch_status)

    batch_download_parser = subparsers.add_parser("batch-download-output", help="Download the output JSONL for the latest or selected batch.")
    batch_download_parser.add_argument("--project", required=True)
    batch_download_parser.add_argument("--project-root", default="projects")
    batch_download_parser.add_argument("--batch-id")
    batch_download_parser.set_defaults(func=cmd_batch_download_output)

    qa_batch_download_parser = subparsers.add_parser("qa-batch-download-output", help="Download the output JSONL for the latest or selected QA batch.")
    qa_batch_download_parser.add_argument("--project", required=True)
    qa_batch_download_parser.add_argument("--project-root", default="projects")
    qa_batch_download_parser.add_argument("--batch-id")
    qa_batch_download_parser.set_defaults(func=cmd_qa_batch_download_output)

    apply_parser = subparsers.add_parser("apply-batch-output", help="Apply a completed batch output file to the EPUB project.")
    apply_parser.add_argument("--project", required=True)
    apply_parser.add_argument("--project-root", default="projects")
    apply_parser.add_argument("--batch-output")
    apply_parser.add_argument("--skip-qa", action="store_true")
    apply_parser.set_defaults(func=cmd_apply_batch_output)

    qa_apply_parser = subparsers.add_parser("apply-qa-output", help="Apply a completed QA batch output file into a QA markdown report.")
    qa_apply_parser.add_argument("--project", required=True)
    qa_apply_parser.add_argument("--project-root", default="projects")
    qa_apply_parser.add_argument("--batch-output")
    qa_apply_parser.set_defaults(func=cmd_apply_qa_output)

    direct_parser = subparsers.add_parser("translate-direct", help="Translate chunks directly via Responses API, for one chapter or the whole book.")
    direct_parser.add_argument("--project", required=True)
    direct_parser.add_argument("--project-root", default="projects")
    direct_parser.add_argument("--chapter")
    direct_parser.add_argument("--max-chunks", type=int, help="Optional global limit for testing. If omitted, translate all remaining chunks.")
    direct_parser.add_argument("--max-chars", type=int)
    direct_parser.add_argument("--retry-only-high", action="store_true", help="When reusing QA_cloud feedback, retry only chunks with at least one high-severity QA issue.")
    direct_parser.set_defaults(func=cmd_translate_direct)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
