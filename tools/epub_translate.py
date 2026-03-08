#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
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
from string import Template
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
    "fr": "French",
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
STRUCTURAL_TRANSLATIONS = {
    "pl": {
        "Sommaire": "Spis treści",
        "Couverture": "Okładka",
        "Page de titre": "Strona tytułowa",
        "Début du texte": "Początek tekstu",
        "Ensemble des notes de pied de page": "Zbiór wszystkich przypisów dolnych",
        "Points de repère": "Punkty orientacyjne",
        "Pages": "Strony",
        "Notes": "Przypisy",
        "Achevé de numériser": "Zakończono skanowanie",
        "Copyright d’origine": "Oryginalna nota copyrightowa",
    },
    "en": {
        "Sommaire": "Table of contents",
        "Couverture": "Cover",
        "Page de titre": "Title page",
        "Début du texte": "Start of text",
        "Ensemble des notes de pied de page": "All footnotes",
        "Points de repère": "Landmarks",
        "Pages": "Pages",
        "Notes": "Notes",
        "Achevé de numériser": "Digitization completed",
        "Copyright d’origine": "Original copyright notice",
    },
}
SOURCE_LANGUAGE_PROFILES = {
    "fr": {
        "markers": FRENCH_MARKERS,
        "stopwords": FRENCH_STOPWORDS,
        "structural_translations": STRUCTURAL_TRANSLATIONS,
    },
}
PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
PROMPT_CACHE: Dict[str, str] = {}
PIPELINE_DEFAULTS = {
    "quality_gate": {"max_high": 0},
    "api_budget_usd": 1.0,
    "targeted_retry_max_chunks": 5,
    "targeted_retry_reasoning_effort": "low",
    "draft_reasoning_effort": None,
    "qa_reasoning_effort": None,
}


def normalize_language_code(code: Optional[str], default: str = "en") -> str:
    return (code or default).split("-", 1)[0].lower()


def language_name(code: Optional[str], default: str = "en") -> str:
    normalized = normalize_language_code(code, default=default)
    return LANGUAGE_NAMES.get(normalized, normalized)


def target_language_name(code: str) -> str:
    return language_name(code, default="en")


def source_language_name(code: Optional[str]) -> str:
    return language_name(code, default="fr")


def source_language_profile(source_language: Optional[str]) -> Dict:
    return SOURCE_LANGUAGE_PROFILES.get(normalize_language_code(source_language, default="fr"), {})


def load_prompt_template(name: str) -> str:
    cached = PROMPT_CACHE.get(name)
    if cached is not None:
        return cached
    path = PROMPTS_DIR / name
    if not path.exists():
        raise RuntimeError(f"Prompt template not found: {path}")
    template = path.read_text(encoding="utf-8")
    PROMPT_CACHE[name] = template
    return template


def render_prompt_template(name: str, **values: str) -> str:
    normalized = {key: ("" if value is None else str(value)) for key, value in values.items()}
    return Template(load_prompt_template(name)).substitute(normalized)


def build_system_prompt(target_language: str, source_language: Optional[str] = None) -> str:
    return render_prompt_template(
        "translation_system.txt",
        language_name=target_language_name(target_language),
        source_language_name=source_language_name(source_language),
    )


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


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


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
        "qa_local": project_dir / "QA_local.md",
        "qa_cloud": project_dir / "QA_cloud.md",
        "qa_changed": project_dir / "QA_changed.md",
        "qa_cloud_history": project_dir / "QA_cloud_history",
        "qa_index": project_dir / "qa_index.json",
        "remediation_plan": project_dir / "remediation_plan.json",
        "iterations_dir": project_dir / "iterations",
        "iteration_log": project_dir / "iterations" / "history.jsonl",
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


def default_project_config(
    epub_path: Path,
    project_dir: Path,
    target_language: str = "en",
    source_language: str = "fr",
) -> Dict:
    paths = project_paths(project_dir, target_language=target_language)
    return {
        "book": {
            "source_epub": str(epub_path.resolve()),
            "project_slug": project_dir.name,
            "output_epub": str(paths["output_epub"].resolve()),
        },
        "translation": {
            "source_language": source_language,
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
            "reasoning_effort": None,
        },
        "pipeline": dict(PIPELINE_DEFAULTS),
        "paths": {
            "glossary": str(paths["glossary"].resolve()),
            "qa": str(paths["qa"].resolve()),
            "progress": str(paths["progress"].resolve()),
            "batch_requests": str(paths["batch_requests"].resolve()),
            "batch_map": str(paths["batch_map"].resolve()),
            "batch_output": str(paths["batch_output"].resolve()),
        },
    }


def write_default_glossary(
    glossary_path: Path,
    target_language: str = "en",
    source_language: str = "fr",
) -> None:
    if glossary_path.exists():
        return
    language_name = target_language_name(target_language)
    source_name = source_language_name(source_language)
    glossary_path.write_text(
        f"# Glossary\n\nSource language: {source_name}\n\n| Source | {language_name} |\n| --- | --- |\n",
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
    config = load_json(config_path, {})
    config.setdefault("translation", {})
    config["translation"].setdefault("source_language", "fr")
    config["translation"].setdefault("target_language", "en")
    config["translation"].setdefault("max_chars_per_chunk", 3200)
    config["translation"].setdefault("preserve_visible_text_only", True)
    config["translation"].setdefault("run_qa_after_apply", True)
    config.setdefault("openai", {})
    config["openai"].setdefault("endpoint", "/v1/responses")
    config["openai"].setdefault("use_batch_api", True)
    config["openai"].setdefault("completion_window", "24h")
    config["openai"].setdefault("temperature", 0.2)
    config["openai"].setdefault("send_temperature", True)
    config["openai"].setdefault("reasoning_effort", None)
    config.setdefault("pipeline", {})
    config["pipeline"].setdefault("quality_gate", {})
    config["pipeline"]["quality_gate"].setdefault("max_high", PIPELINE_DEFAULTS["quality_gate"]["max_high"])
    for key, value in PIPELINE_DEFAULTS.items():
        if key == "quality_gate":
            continue
        config["pipeline"].setdefault(key, value)
    return config


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
    parts = [normalize_space(target.original_text) for target in targets if normalize_space(target.original_text)]
    return normalize_space(" ".join(parts))


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
    parts = [
        normalize_space(translated_lookup.get((target.xpath, target.field), ""))
        for target in unit.targets
        if normalize_space(translated_lookup.get((target.xpath, target.field), ""))
    ]
    return normalize_space(" ".join(parts))


def render_unit_translation_with_placeholders(
    unit: TextUnit,
    translated_lookup: Dict[Tuple[str, str], str],
) -> str:
    parts: List[str] = []
    for index, target in enumerate(unit.targets):
        parts.append(translated_lookup.get((target.xpath, target.field), ""))
        if index < len(unit.targets) - 1:
            parts.append(segment_placeholder(index + 1))
    return "".join(parts)


def structural_translation(source_text: str, source_language: Optional[str], target_language: str) -> Optional[str]:
    normalized = normalize_space(source_text).replace("\xa0", " ")
    language = normalize_language_code(target_language, default="en")
    mapping = source_language_profile(source_language).get("structural_translations", {}).get(language, {})
    if normalized in mapping:
        return mapping[normalized]
    page_match = re.fullmatch(r"Page\s+(\d+)", normalized)
    if page_match:
        if language == "pl":
            return f"Strona {page_match.group(1)}"
        if language == "en":
            return f"Page {page_match.group(1)}"
    return None


def extract_number_tokens(text: str) -> List[str]:
    return re.findall(r"\b\d{1,4}\b", text)


def has_obvious_number_mismatch(source_text: str, translation_text: str) -> bool:
    source_numbers = extract_number_tokens(source_text)
    translation_numbers = extract_number_tokens(translation_text)
    return bool(source_numbers and translation_numbers and source_numbers != translation_numbers)


def replace_number_tokens_from_source(source_text: str, translation_text: str) -> str:
    source_numbers = extract_number_tokens(source_text)
    translation_numbers = extract_number_tokens(translation_text)
    if not source_numbers or len(source_numbers) != len(translation_numbers):
        return translation_text
    iterator = iter(source_numbers)
    return re.sub(r"\b\d{1,4}\b", lambda _: next(iterator), translation_text, count=len(source_numbers))


def normalize_spacing_artifacts(text: str) -> str:
    fixed = text
    fixed = re.sub(r"\s+([,.;:!?])", r"\1", fixed)
    fixed = re.sub(r"([(\[„])\s+", r"\1", fixed)
    fixed = re.sub(r"\s+([)\]”])", r"\1", fixed)
    fixed = re.sub(r" {2,}", " ", fixed)
    return fixed


def normalize_matching_text(text: str) -> str:
    normalized = text.replace("\xa0", " ")
    normalized = re.sub(r"\[\[SEG_\d+\]\]", " ", normalized)
    normalized = normalize_space(normalized)
    normalized = re.sub(r"(\d)\s+er\b", r"\1er", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\b([IVXLCDM]+)\s+e\b", r"\1e", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+([,.;:!?])", r"\1", normalized)
    return normalized


def extract_note_suggestions(note: str) -> List[str]:
    suggestions: List[str] = []
    for pattern in (
        r'"([^"]+)"',
        r"'([^']+)'",
        r"„([^”]+)”",
        r"“([^”]+)”",
        r"«([^»]+)»",
    ):
        suggestions.extend(normalize_space(match) for match in re.findall(pattern, note or ""))
    deduped: List[str] = []
    for item in suggestions:
        if item and item not in deduped:
            deduped.append(item)
    return deduped


def issue_matches_unit(issue: Dict[str, str], unit: TextUnit, translation_text: str) -> bool:
    source_excerpt = normalize_matching_text(issue.get("source_excerpt", ""))
    translation_excerpt = normalize_matching_text(issue.get("translation_excerpt", ""))
    source_candidates = {normalize_matching_text(unit.plain_text), normalize_matching_text(unit.text)}
    translation_candidate = normalize_matching_text(translation_text)
    if source_excerpt and any(source_excerpt in candidate for candidate in source_candidates if candidate):
        return True
    if translation_excerpt and translation_excerpt in translation_candidate:
        return True
    return not source_excerpt and not translation_excerpt


def reasoned_effort_for_mode(config: Dict, mode: str) -> Optional[str]:
    pipeline = config.get("pipeline", {})
    if mode == "draft":
        return pipeline.get("draft_reasoning_effort")
    if mode == "targeted_retry":
        return pipeline.get("targeted_retry_reasoning_effort")
    if mode == "qa":
        return pipeline.get("qa_reasoning_effort")
    return config.get("openai", {}).get("reasoning_effort")


def should_use_qa_feedback(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "reuse_qa_feedback", False)
        or getattr(args, "retry_only_high", False)
        or getattr(args, "qa_snapshot", None)
    )


def load_glossary(glossary_path: Path) -> Dict[str, str]:
    glossary = {}
    if not glossary_path.exists():
        return glossary
    for line in glossary_path.read_text(encoding="utf-8").splitlines():
        if "|" not in line:
            continue
        parts = [part.strip() for part in line.strip("|").split("|")]
        if len(parts) < 2 or not parts[0] or not parts[1]:
            continue
        if set(parts[0]) == {"-"} or set(parts[1]) == {"-"}:
            continue
        if parts[0].casefold().startswith("source") or parts[0].casefold() == "french":
            continue
        glossary[parts[0]] = parts[1]
    return glossary


def extract_inline_glossary_candidates(tree: ET.ElementTree, source_language: Optional[str] = None) -> List[str]:
    stopwords = source_language_profile(source_language).get("stopwords", set())
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
        if stopwords and len(words) == 1 and words[0].lower() in stopwords:
            continue
        candidates.append(phrase)
    return candidates


def extract_repeated_term_candidates(text: str, source_language: Optional[str] = None) -> List[str]:
    stopwords = source_language_profile(source_language).get("stopwords", set())
    words = re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'’-]*", text)
    candidates: List[str] = []
    max_start = max(0, len(words) - 1)
    for size in (2, 3):
        for start in range(0, max(0, len(words) - size + 1)):
            phrase_words = words[start:start + size]
            lowered = [word.lower() for word in phrase_words]
            if stopwords and (lowered[0] in stopwords or lowered[-1] in stopwords):
                continue
            if stopwords and all(word in stopwords for word in lowered):
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
    source_language: Optional[str] = None,
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
        for phrase in extract_inline_glossary_candidates(tree, source_language=source_language):
            record(phrase, "inline emphasis", href)
        for phrase in extract_named_entity_candidates(plain_text):
            record(phrase, "capitalized phrase", href)
        for phrase in extract_repeated_term_candidates(plain_text, source_language=source_language):
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
        f"| Source | Suggested {language_name} | Count | Why | Seen In |",
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


def latest_qa_history_snapshot(qa_history_dir: Path) -> Optional[Path]:
    if not qa_history_dir.exists():
        return None
    files = [path for path in qa_history_dir.glob("*.md") if path.is_file()]
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)


def qa_snapshot_metadata(paths: Dict[str, Path]) -> Dict[str, Optional[str]]:
    qa_cloud_path = paths["qa_cloud"]
    latest_history = latest_qa_history_snapshot(paths["qa_cloud_history"])
    current_text = qa_cloud_path.read_text(encoding="utf-8") if qa_cloud_path.exists() else ""
    finding_headers = re.findall(r"^###\s+.+?\s+chunk\s+\d+$", current_text, flags=re.MULTILINE)
    return {
        "qa_cloud_path": str(qa_cloud_path),
        "qa_cloud_sha256": sha256_text(current_text) if current_text else None,
        "qa_cloud_updated_at": dt.datetime.fromtimestamp(qa_cloud_path.stat().st_mtime, tz=dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z") if qa_cloud_path.exists() else None,
        "qa_cloud_chunks_with_findings": len(finding_headers) if current_text else 0,
        "latest_history_snapshot": str(latest_history) if latest_history else None,
        "latest_history_sha256": file_sha256(latest_history) if latest_history else None,
    }


def append_iteration_event(project_dir: Path, event_type: str, payload: Dict) -> Dict:
    paths = project_paths(project_dir)
    ensure_dir(paths["iterations_dir"])
    event = {
        "timestamp": utc_now_iso(),
        "event": event_type,
        "project": project_dir.name,
        "payload": payload,
    }
    with paths["iteration_log"].open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")
    return event


def load_iteration_events(project_dir: Path) -> List[Dict]:
    log_path = project_paths(project_dir)["iteration_log"]
    if not log_path.exists():
        return []
    events: List[Dict] = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


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
        "- Treat every issue below as a mandatory correction target in this retranslation.",
        "- Resolve the underlying source-meaning or wording problem, not just the surface phrasing called out by QA.",
        "- Do not preserve broken phrasing from the previous attempt.",
    ]
    for issue in issues:
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


def summarize_qa_scope(requests: List[Dict], all_files: Optional[List[str]] = None) -> Dict:
    hrefs = sorted({request.get("href") for request in requests if request.get("href")})
    chunk_count = len([request for request in requests if request.get("href")])
    all_files_normalized = sorted(all_files or [])
    scope_type = "empty"
    if hrefs:
        if all_files_normalized and hrefs == all_files_normalized:
            scope_type = "full"
        elif len(hrefs) == 1:
            scope_type = "chapter"
        else:
            scope_type = "partial"
    return {
        "scope_type": scope_type,
        "hrefs": hrefs,
        "chapter_count": len(hrefs),
        "chunk_count": chunk_count,
        "all_files": all_files_normalized,
    }


def load_qa_index(paths: Dict[str, Path]) -> Dict:
    return load_json(paths["qa_index"], {"active_snapshot": None, "snapshots": []})


def save_qa_index(paths: Dict[str, Path], data: Dict) -> None:
    save_json(paths["qa_index"], data)


def register_qa_snapshot(
    project_dir: Path,
    snapshot_path: Path,
    requests: List[Dict],
    checked_chunks: int,
    findings: int,
    failed_chunks: int,
    scope_override: Optional[Dict] = None,
    make_active: bool = True,
) -> Dict:
    paths = project_paths(project_dir)
    all_files = visible_content_files(paths["source_dir"]) if paths["source_dir"].exists() else []
    scope = dict(scope_override or summarize_qa_scope(requests, all_files=all_files))
    if scope_override and all_files and not scope.get("all_files"):
        scope["all_files"] = all_files
    snapshot = {
        "path": str(snapshot_path),
        "sha256": file_sha256(snapshot_path),
        "created_at": utc_now_iso(),
        "checked_chunks": checked_chunks,
        "findings": findings,
        "failed_chunks": failed_chunks,
        **scope,
    }
    index = load_qa_index(paths)
    snapshots = [item for item in index.get("snapshots", []) if item.get("path") != snapshot["path"]]
    snapshots.append(snapshot)
    index["snapshots"] = snapshots[-50:]
    if make_active:
        index["active_snapshot"] = snapshot
    save_qa_index(paths, index)
    return snapshot


def infer_qa_snapshot_scope(snapshot_path: Path, source_dir: Path) -> Dict:
    feedback = load_qa_feedback(snapshot_path)
    requests = []
    for href, chunks in feedback.items():
        for chunk_index in chunks:
            requests.append({"href": href, "chunk_index": chunk_index})
    all_files = visible_content_files(source_dir) if source_dir.exists() else []
    scope = summarize_qa_scope(requests, all_files=all_files)
    return {
        "path": str(snapshot_path),
        "sha256": file_sha256(snapshot_path),
        "created_at": dt.datetime.fromtimestamp(snapshot_path.stat().st_mtime, tz=dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z") if snapshot_path.exists() else None,
        "checked_chunks": scope["chunk_count"],
        "findings": sum(len(chunks) for chunks in feedback.values()),
        "failed_chunks": 0,
        **scope,
    }


def resolve_qa_snapshot(
    project_dir: Path,
    selected_files: List[str],
    explicit_snapshot: Optional[str] = None,
    allow_partial_retry: bool = False,
) -> Tuple[Path, Dict, Dict[str, Dict[int, Dict]]]:
    paths = project_paths(project_dir)
    source_dir = paths["source_dir"]
    index = load_qa_index(paths)
    snapshot_meta: Optional[Dict] = None

    if explicit_snapshot:
        snapshot_path = Path(explicit_snapshot)
        if not snapshot_path.is_absolute():
            snapshot_path = (project_dir / snapshot_path).resolve()
        if not snapshot_path.exists():
            raise RuntimeError(f"QA snapshot not found: {snapshot_path}")
        snapshot_meta = infer_qa_snapshot_scope(snapshot_path, source_dir)
    else:
        active_snapshot = index.get("active_snapshot")
        snapshots = index.get("snapshots", [])
        if len(selected_files) == 1:
            target_href = selected_files[0]
            candidates = [
                item for item in snapshots
                if target_href in item.get("hrefs", []) or item.get("scope_type") == "full"
            ]
            if active_snapshot and (target_href in active_snapshot.get("hrefs", []) or active_snapshot.get("scope_type") == "full"):
                snapshot_meta = active_snapshot
            elif candidates:
                snapshot_meta = max(candidates, key=lambda item: item.get("created_at", ""))
        else:
            full_snapshots = [item for item in snapshots if item.get("scope_type") == "full"]
            if active_snapshot and active_snapshot.get("scope_type") == "full":
                snapshot_meta = active_snapshot
            elif full_snapshots:
                snapshot_meta = max(full_snapshots, key=lambda item: item.get("created_at", ""))
            elif active_snapshot:
                snapshot_meta = active_snapshot
        if not snapshot_meta:
            snapshot_path = paths["qa_cloud"]
            if not snapshot_path.exists():
                return snapshot_path, {"scope_type": "empty", "hrefs": [], "chapter_count": 0, "chunk_count": 0}, {}
            snapshot_meta = infer_qa_snapshot_scope(snapshot_path, source_dir)
        snapshot_path = Path(snapshot_meta["path"])

    if not snapshot_path.exists():
        raise RuntimeError(f"Resolved QA snapshot does not exist: {snapshot_path}")

    snapshot_hrefs = set(snapshot_meta.get("hrefs", []))
    full_book_run = len(selected_files) > 1
    if full_book_run and snapshot_meta.get("scope_type") not in {"full", "empty"} and not allow_partial_retry:
        raise RuntimeError(
            "Refusing whole-book retry with a partial QA snapshot. "
            "Run with --qa-snapshot <full_report.md>, or use --allow-partial-qa-retry, or target one chapter with --chapter."
        )
    if len(selected_files) == 1 and snapshot_meta.get("scope_type") not in {"full", "empty"}:
        target_href = selected_files[0]
        if target_href not in snapshot_hrefs:
            raise RuntimeError(
                f"QA snapshot {snapshot_path} does not cover requested chapter {target_href}. "
                "Use --qa-snapshot with a matching report or run without QA-guided retry."
            )

    feedback = load_qa_feedback(snapshot_path)
    return snapshot_path, snapshot_meta, feedback


def generate_qa_report(
    translated_files: Iterable[Path],
    glossary: Dict[str, str],
    qa_path: Path,
    source_language: Optional[str] = None,
) -> None:
    markers = source_language_profile(source_language).get("markers", set())
    lines = ["# QA Report", "", "## Checks", ""]
    for file_path in translated_files:
        status = {
            "xml_ok": "PASS",
            "source_leftovers": "PASS",
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
            if markers and sum(marker in lowered for marker in markers) >= 3:
                status["source_leftovers"] = "WARN"
                notes.append(f"{source_language_name(source_language)} marker heuristics detected residual source-language text.")
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
                "input": build_qa_payload(
                    pairs,
                    config["translation"].get("source_language", "fr"),
                    config["translation"].get("target_language", "en"),
                ),
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


def resolve_snapshot_for_manifest(project_dir: Path, explicit_snapshot: Optional[str] = None) -> Tuple[Path, Dict]:
    paths = project_paths(project_dir)
    if explicit_snapshot:
        snapshot_path = Path(explicit_snapshot)
        if not snapshot_path.is_absolute():
            snapshot_path = (project_dir / snapshot_path).resolve()
        if not snapshot_path.exists():
            raise RuntimeError(f"QA snapshot not found: {snapshot_path}")
        return snapshot_path, infer_qa_snapshot_scope(snapshot_path, paths["source_dir"])
    index = load_qa_index(paths)
    active_snapshot = index.get("active_snapshot")
    if active_snapshot:
        snapshot_path = Path(active_snapshot["path"])
        if snapshot_path.exists():
            return snapshot_path, active_snapshot
    if paths["qa_cloud"].exists():
        return paths["qa_cloud"], infer_qa_snapshot_scope(paths["qa_cloud"], paths["source_dir"])
    raise RuntimeError("No QA snapshot available. Run cloud QA first or pass --qa-snapshot.")


def structural_file_hint(href: str) -> bool:
    return href.endswith(("toc.xhtml", "toc01.xhtml", "tp01.xhtml", "cop01.xhtml", "acheve_12.xhtml", "ftn01.xhtml"))


def classify_issue_fix_mode(
    issue: Dict[str, str],
    href: str,
    source_language: Optional[str],
    target_language: str,
) -> Tuple[str, str]:
    category = issue.get("category", "")
    note = (issue.get("note", "") or "").lower()
    source_excerpt = issue.get("source_excerpt", "") or ""
    translation_excerpt = issue.get("translation_excerpt", "") or ""
    structural_hint = structural_file_hint(href) or structural_translation(source_excerpt, source_language, target_language) is not None
    numeric_mismatch = has_obvious_number_mismatch(source_excerpt, translation_excerpt)

    if category == "leftover_french":
        return "local", f"leftover {source_language_name(source_language)} / marker cleanup"
    if category == "formatting":
        if structural_hint or any(token in note for token in ("spacing", "spacj", "odstęp", "glued", "zlane", "sklej", "missing space", "punctuation", "typograph")):
            return "local", "deterministic formatting cleanup"
        return "model", "formatting issue still needs semantic rewrite"
    if category == "accuracy":
        if structural_hint:
            return "local", "structural or metadata label mismatch"
        if numeric_mismatch or any(token in note for token in ("year", "rok", "date", "liczb", "fact", "faktografic")):
            return "local", "numeric/date mismatch"
        return "model", "semantic accuracy issue"
    if category == "terminology":
        if structural_hint:
            return "local", "structural terminology fix"
        return "model", "terminology issue in prose"
    if category == "fluency":
        if any(token in note for token in ("typo", "spacj", "spacing", "broken polish syntax", "text damage", "uszkodzona składnia")):
            return "local", "surface fluency cleanup"
        return "model", "fluency issue in prose"
    return "model", "default to model/manual remediation"


def qa_issue_counts(feedback: Dict[str, Dict[int, Dict]]) -> Dict[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0}
    for chunks in feedback.values():
        for chunk_feedback in chunks.values():
            for issue in chunk_feedback.get("issues") or []:
                severity = qa_issue_effective_severity(issue)
                if severity in counts:
                    counts[severity] += 1
    return counts


def qa_issue_effective_severity(issue: Dict) -> str:
    severity = issue.get("effective_severity") or issue.get("severity") or "low"
    if severity != "high":
        return severity
    note = (issue.get("note") or "").lower()
    category = (issue.get("category") or "").lower()
    translation_excerpt = (issue.get("translation_excerpt") or "").lower()
    if "omit if strictly source-based" in note:
        return "medium"
    if "nie zgłaszać" in note:
        return "medium"
    if "wiernie oddaje francuski" in note:
        return "medium"
    if "source segment is textually corrupted" in note:
        return "medium"
    if "source segment is textually damaged" in note:
        return "medium"
    if "nosi ślad uszkodzenia" in note:
        return "medium"
    if "text damage rather than a deliberate omission" in note:
        return "medium"
    if "masking source damage" in note:
        return "medium"
    if category == "leftover_french" and any(token in translation_excerpt for token in ("école", "société française")):
        return "medium"
    return severity


def qa_issue_gate_reason(issue: Dict) -> Optional[str]:
    if (issue.get("severity") or "low") != "high":
        return None
    note = (issue.get("note") or "").lower()
    category = (issue.get("category") or "").lower()
    translation_excerpt = (issue.get("translation_excerpt") or "").lower()
    if "omit if strictly source-based" in note:
        return "qa_self_negated_source_based"
    if "nie zgłaszać" in note or "wiernie oddaje francuski" in note:
        return "qa_self_negated_source_based"
    if "source segment is textually corrupted" in note or "source segment is textually damaged" in note:
        return "source_text_corruption"
    if "nosi ślad uszkodzenia" in note or "masking source damage" in note:
        return "source_text_corruption"
    if "text damage rather than a deliberate omission" in note:
        return "source_text_damage"
    if category == "leftover_french" and any(token in translation_excerpt for token in ("école", "société française")):
        return "retained_institution_name"
    return None


def annotate_qa_issues_for_gate(issues: List[Dict]) -> List[Dict]:
    annotated: List[Dict] = []
    for issue in issues:
        annotated_issue = dict(issue)
        annotated_issue["effective_severity"] = qa_issue_effective_severity(annotated_issue)
        gate_reason = qa_issue_gate_reason(annotated_issue)
        if gate_reason:
            annotated_issue["gate_reason"] = gate_reason
        annotated.append(annotated_issue)
    return annotated


def build_remediation_manifest(project_dir: Path, config: Dict, snapshot_path: Path, snapshot_meta: Dict) -> Dict:
    feedback = load_qa_feedback(snapshot_path)
    source_language = config["translation"].get("source_language", "fr")
    target_language = config["translation"].get("target_language", "en")
    chunks: List[Dict] = []
    baseline_counts = qa_issue_counts(feedback)
    for href in sorted(feedback):
        for chunk_index in sorted(feedback[href]):
            chunk_feedback = feedback[href][chunk_index]
            high_issues = [
                issue
                for issue in annotate_qa_issues_for_gate(chunk_feedback.get("issues") or [])
                if qa_issue_effective_severity(issue) == "high"
            ]
            if not high_issues:
                continue
            manifest_issues = []
            chunk_modes = []
            for issue in high_issues:
                fix_mode, reason = classify_issue_fix_mode(issue, href, source_language, target_language)
                manifest_issues.append(
                    {
                        **issue,
                        "fix_mode": fix_mode,
                        "fix_reason": reason,
                    }
                )
                chunk_modes.append(fix_mode)
            chunk_fix_mode = "local" if chunk_modes and all(mode == "local" for mode in chunk_modes) else "model"
            chunks.append(
                {
                    "chunk_key": f"{href}::chunk::{chunk_index:04d}",
                    "href": href,
                    "chunk_index": chunk_index,
                    "summary": chunk_feedback.get("summary") or "High-severity QA issues",
                    "chunk_fix_mode": chunk_fix_mode,
                    "issues": manifest_issues,
                    "local_status": "pending" if any(issue["fix_mode"] == "local" for issue in manifest_issues) else "not_applicable",
                    "model_status": "pending" if any(issue["fix_mode"] == "model" for issue in manifest_issues) else "not_applicable",
                    "qa_changed_status": "pending",
                    "changed": False,
                    "last_error": None,
                }
            )
    local_chunks = len([chunk for chunk in chunks if chunk["local_status"] != "not_applicable"])
    model_chunks = len([chunk for chunk in chunks if chunk["model_status"] != "not_applicable"])
    return {
        "project": project_dir.name,
        "created_at": utc_now_iso(),
        "qa_snapshot": {
            **snapshot_meta,
            "path": str(snapshot_path),
            "sha256": file_sha256(snapshot_path),
        },
        "quality_gate": config["pipeline"]["quality_gate"],
        "api_budget_usd": config["pipeline"]["api_budget_usd"],
        "targeted_retry_max_chunks": config["pipeline"]["targeted_retry_max_chunks"],
        "targeted_retry_rounds": 0,
        "estimated_spend_usd": 0.0,
        "stop_loss_triggered": None,
        "summary": {
            "baseline_counts": baseline_counts,
            "selected_high_chunks": len(chunks),
            "local_chunks": local_chunks,
            "model_chunks": model_chunks,
        },
        "chunks": chunks,
        "qa_changed": None,
    }


def load_remediation_plan(plan_path: Path) -> Dict:
    if not plan_path.exists():
        raise RuntimeError(f"Remediation plan not found: {plan_path}")
    return load_json(plan_path, {})


def save_remediation_plan(plan_path: Path, data: Dict) -> None:
    save_json(plan_path, data)


def verify_frozen_snapshot(manifest: Dict) -> None:
    snapshot = manifest.get("qa_snapshot") or {}
    snapshot_path = Path(snapshot.get("path", ""))
    if snapshot_path.exists():
        current_sha = file_sha256(snapshot_path)
        expected_sha = snapshot.get("sha256")
        if expected_sha and current_sha != expected_sha:
            raise RuntimeError(
                f"Frozen QA snapshot changed since remediation plan creation: {snapshot_path}"
            )


def progress_lookup_for_href(progress: Dict, href: str) -> Dict[Tuple[str, str], str]:
    return {
        tuple(key.split("::", 1)): value
        for key, value in progress.get("translations", {}).get(href, {}).items()
    }


def save_translated_lookup_for_href(
    paths: Dict[str, Path],
    progress: Dict,
    href: str,
    translated_lookup: Dict[Tuple[str, str], str],
) -> Path:
    progress["translations"][href] = {
        f"{xpath}::{field}": value for (xpath, field), value in translated_lookup.items()
    }
    source_file = paths["source_dir"] / href
    target_file = paths["translated_dir"] / href
    tree, _ = collect_text_units(source_file)
    assign_translations(tree, translated_lookup)
    ensure_dir(target_file.parent)
    tree.write(target_file, encoding="utf-8", xml_declaration=True)
    return target_file


def load_chunk_from_source(paths: Dict[str, Path], href: str, chunk_index: int, max_chars: int) -> List[TextUnit]:
    source_file = paths["source_dir"] / href
    _, units = collect_text_units(source_file)
    chunks = chunk_units(units, max_chars)
    if chunk_index >= len(chunks):
        raise RuntimeError(f"Chunk {chunk_index} out of range for {href}")
    return chunks[chunk_index]


def repair_formatting_excerpt(excerpt: str, note: str) -> Optional[str]:
    normalized_excerpt = normalize_space(excerpt)
    if not normalized_excerpt:
        return None
    suggestions = extract_note_suggestions(note)
    if len(suggestions) == 1 and suggestions[0] != normalized_excerpt:
        return suggestions[0]
    lowered_note = (note or "").lower()
    if "missing space" in lowered_note or "brak spacji" in lowered_note:
        match = re.match(
            r"^(do|na|od|po|za|bez|przy|pod|nad|nie)([A-Za-zÀ-ÿąćęłńóśźżĄĆĘŁŃÓŚŹŻ-]{4,})$",
            normalized_excerpt,
        )
        if match:
            return f"{match.group(1)} {match.group(2)}"
    normalized = normalize_spacing_artifacts(normalized_excerpt)
    if normalized != normalized_excerpt:
        return normalized
    return None


def apply_local_fixes_to_unit(
    unit: TextUnit,
    current_translation: str,
    issues: List[Dict[str, str]],
    source_language: Optional[str],
    target_language: str,
) -> str:
    if not issues:
        return current_translation
    updated = current_translation
    applicable = [issue for issue in issues if issue_matches_unit(issue, unit, current_translation)]
    if not applicable:
        return updated

    if len(unit.targets) == 1:
        mapped = structural_translation(unit.plain_text, source_language, target_language)
        if mapped and any(issue.get("fix_mode") == "local" for issue in applicable):
            updated = preserve_outer_whitespace(unit.targets[0].original_text, mapped)

    if target_language.split("-", 1)[0].lower() == "pl":
        updated = re.sub(r"(\d+)\s*(\[\[SEG_\d+\]\])\s*er\b", r"\1\2", updated)
        updated = re.sub(r"(\d+)er\b", r"\1", updated)
        updated = re.sub(r"([IVXLCDM]+)\s*(\[\[SEG_\d+\]\])\s*e\b", r"\1\2", updated)
        updated = re.sub(r"([IVXLCDM]+)e\b", r"\1", updated)

    if has_obvious_number_mismatch(unit.text, updated):
        updated = replace_number_tokens_from_source(unit.text, updated)

    for issue in applicable:
        excerpt = issue.get("translation_excerpt", "") or ""
        replacement = repair_formatting_excerpt(excerpt, issue.get("note", "") or "")
        if replacement and excerpt and excerpt in updated:
            updated = updated.replace(excerpt, replacement)
            continue
        normalized_excerpt = normalize_matching_text(excerpt)
        normalized_updated = normalize_matching_text(updated)
        if replacement and normalized_excerpt and normalized_excerpt == normalized_updated:
            updated = replacement
            continue
        if replacement and normalized_excerpt and len(unit.targets) == 1 and normalized_excerpt in normalized_updated:
            updated = replacement

    updated = normalize_spacing_artifacts(updated)
    return updated


def estimate_direct_request_cost(model: str, input_chars: int, output_chars: int) -> float:
    pricing = PRICE_TABLE.get(model)
    if not pricing:
        return 0.0
    input_tokens = math.ceil(input_chars / 4)
    output_tokens = math.ceil(output_chars / 4)
    input_cost = input_tokens / 1_000_000 * pricing["input"]
    output_cost = output_tokens / 1_000_000 * pricing["output"]
    return input_cost + output_cost


def qa_counts_from_issue_list(issues: List[Dict]) -> Dict[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0}
    for issue in issues:
        severity = qa_issue_effective_severity(issue)
        if severity in counts:
            counts[severity] += 1
    return counts


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
    source_language: Optional[str],
    target_language: str,
    qa_feedback_text: str = "",
    strong_formatting_retry: bool = False,
) -> List[Dict]:
    language_name = target_language_name(target_language)
    source_name = source_language_name(source_language)
    glossary_lines = [f"- {fr} => {en}" for fr, en in glossary.items()]
    glossary_text = "\n".join(glossary_lines) if glossary_lines else "- none yet"
    units = [{"id": index, "text": normalize_space(unit.text)} for index, unit in enumerate(chunk)]
    feedback_block = ""
    if qa_feedback_text:
        feedback_block = (
            "\nThis is a retry of a previously translated chunk.\n"
            f"Use the QA findings below as a mandatory correction checklist while translating from the {source_name} source again.\n"
            f"The old translation may be wrong; retranslate from the {source_name} and fix the underlying issues rather than paraphrasing the QA notes.\n"
            "If QA reported formatting or broken-syntax damage, reconstruct a fluent target sentence while still returning one translation per segment id.\n"
            "Before finalizing, silently verify that none of the listed QA defects remain in your new translation.\n"
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
    user_prompt = render_prompt_template(
        "translation_user.txt",
        source_language_name=source_name,
        language_name=language_name,
        formatting_retry_block=formatting_retry_block,
        feedback_block=feedback_block,
        glossary_text=glossary_text,
        segments_json=json.dumps(units, ensure_ascii=False),
    )
    return [
        {"role": "system", "content": build_system_prompt(target_language, source_language=source_language)},
        {"role": "user", "content": user_prompt},
    ]


def build_qa_payload(
    pairs: List[Dict[str, str]],
    source_language: Optional[str],
    target_language: str,
) -> List[Dict]:
    language_name = target_language_name(target_language)
    source_name = source_language_name(source_language)
    system_prompt = render_prompt_template(
        "qa_system.txt",
        source_language_name=source_name,
        language_name=language_name,
    )
    user_prompt = render_prompt_template(
        "qa_user.txt",
        source_language_name=source_name,
        language_name=language_name,
        segments_json=json.dumps(pairs, ensure_ascii=False),
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


def assemble_final_epub(project_dir: Path, config: Dict) -> Dict[str, str]:
    paths = project_paths(project_dir)
    target_language = config["translation"].get("target_language", "en")
    normalize_translated_package(paths["translated_dir"], target_language)
    sync_navigation_documents(paths["translated_dir"])
    rezip_epub(paths["translated_dir"], paths["output_epub"])
    return {
        "project": project_dir.name,
        "output_epub": str(paths["output_epub"]),
        "translated_dir": str(paths["translated_dir"]),
    }


def text_unit_from_dict(data: Dict) -> TextUnit:
    return TextUnit(
        xpath=data["xpath"],
        field=data["field"],
        text=data["text"],
        plain_text=data.get("plain_text") or normalize_space(" ".join(target["original_text"] for target in data["targets"])),
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
    source_language = args.source_language or "fr"
    target_language = args.target_language or "en"
    project_dir = infer_project_dir(args.project, args.epub, project_root, target_language=target_language)
    if project_dir.exists() and any(project_dir.iterdir()) and not args.force:
        raise RuntimeError(f"Project directory already exists: {project_dir}")
    ensure_dir(project_dir)
    paths = project_paths(project_dir, target_language=target_language)
    for key in ("project_dir", "source_dir", "translated_dir", "batch_requests", "batch_map"):
        if isinstance(paths[key], Path):
            ensure_dir(paths[key].parent if paths[key].suffix else paths[key])
    config = default_project_config(
        epub_path,
        project_dir,
        target_language=target_language,
        source_language=source_language,
    )
    save_project_config(project_dir, config)
    write_default_glossary(paths["glossary"], target_language=target_language, source_language=source_language)
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
    if args.source_language is not None:
        config["translation"]["source_language"] = args.source_language
    if args.target_language is not None:
        config["translation"]["target_language"] = args.target_language
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


def cmd_draft(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    if config["openai"].get("use_batch_api", True):
        cmd_run_batch(
            argparse.Namespace(
                project=args.project,
                project_root=args.project_root,
                chapter=args.chapter,
                max_chars=args.max_chars,
                reuse_qa_feedback=False,
                retry_only_high=False,
                qa_snapshot=None,
                allow_partial_qa_retry=False,
            )
        )
        return
    cmd_translate_direct(
        argparse.Namespace(
            project=args.project,
            project_root=args.project_root,
            chapter=args.chapter,
            max_chunks=args.max_chunks,
            max_chars=args.max_chars,
            reuse_qa_feedback=False,
            retry_only_high=False,
            qa_snapshot=None,
            allow_partial_qa_retry=False,
        )
    )


def cmd_review(args: argparse.Namespace) -> None:
    cmd_validate_local(
        argparse.Namespace(
            project=args.project,
            project_root=args.project_root,
            chapter=args.chapter,
            max_chars=args.max_chars,
        )
    )


def cmd_finalize(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    result = assemble_final_epub(project_dir, config)
    print(json.dumps(result, ensure_ascii=False, indent=2))


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
        source_language=config["translation"].get("source_language", "fr"),
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


def cmd_iteration_status(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    events = load_iteration_events(project_dir)
    limit = max(args.limit, 1)
    selected = events[-limit:]
    print(json.dumps({"project": project_dir.name, "events": selected}, ensure_ascii=False, indent=2))


def cmd_prepare_batch(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    model = config["openai"].get("model")
    if not model:
        raise RuntimeError("Set a model first with configure-openai.")
    paths = project_paths(project_dir)
    source_dir = paths["source_dir"]
    glossary = load_glossary(paths["glossary"])
    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))
    selected_files = [args.chapter] if args.chapter else visible_content_files(source_dir)
    if should_use_qa_feedback(args):
        qa_snapshot_path, qa_snapshot_meta, qa_feedback = resolve_qa_snapshot(
            project_dir,
            selected_files,
            explicit_snapshot=args.qa_snapshot,
            allow_partial_retry=args.allow_partial_qa_retry,
        )
    else:
        qa_snapshot_path = paths["qa_cloud"]
        qa_snapshot_meta = {"scope_type": "disabled", "hrefs": [], "chapter_count": 0, "chunk_count": 0}
        qa_feedback = {}

    requests: List[str] = []
    request_map = {
        "project": project_dir.name,
        "qa_snapshot_path": str(qa_snapshot_path),
        "qa_snapshot_meta": qa_snapshot_meta,
        "requests": [],
    }
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
                    config["translation"].get("source_language", "fr"),
                    config["translation"].get("target_language", "en"),
                    qa_feedback_text=qa_feedback_text,
                    strong_formatting_retry=strong_formatting_retry,
                ),
            }
            temperature = config["openai"].get("temperature")
            if config["openai"].get("send_temperature", True) and temperature is not None:
                body["temperature"] = temperature
            reasoning_effort = reasoned_effort_for_mode(config, "targeted_retry" if qa_feedback_text else "draft")
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
    print(
        json.dumps(
            {
                "requests": len(requests),
                "batch_requests": str(paths["batch_requests"]),
                "qa_snapshot_path": str(qa_snapshot_path),
                "qa_scope": qa_snapshot_meta.get("scope_type"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


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
    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))
    selected_files = [args.chapter] if args.chapter else visible_content_files(source_dir)
    if should_use_qa_feedback(args):
        qa_snapshot_path, qa_snapshot_meta, qa_feedback = resolve_qa_snapshot(
            project_dir,
            selected_files,
            explicit_snapshot=args.qa_snapshot,
            allow_partial_retry=args.allow_partial_retry if hasattr(args, "allow_partial_retry") else args.allow_partial_qa_retry,
        )
    else:
        qa_snapshot_path = paths["qa_cloud"]
        qa_snapshot_meta = {"scope_type": "disabled", "hrefs": [], "chapter_count": 0, "chunk_count": 0}
        qa_feedback = {}
    max_chars = args.max_chars or config["translation"]["max_chars_per_chunk"]

    requests: List[str] = []
    request_map = {
        "project": project_dir.name,
        "qa_snapshot_path": str(qa_snapshot_path),
        "qa_snapshot_meta": qa_snapshot_meta,
        "requests": [],
    }
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
                    config["translation"].get("source_language", "fr"),
                    config["translation"].get("target_language", "en"),
                    qa_feedback_text=qa_feedback_text,
                    strong_formatting_retry=strong_formatting_retry,
                ),
            }
            temperature = config["openai"].get("temperature")
            if config["openai"].get("send_temperature", True) and temperature is not None:
                body["temperature"] = temperature
            reasoning_effort = reasoned_effort_for_mode(config, "targeted_retry" if qa_feedback_text else "draft")
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
        f"(qa_retry_chunks={qa_retry_chunks}, strong_formatting_retries={strong_formatting_retries}, "
        f"qa_scope={qa_snapshot_meta.get('scope_type')}, qa_snapshot={qa_snapshot_path})"
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
    append_iteration_event(
        project_dir,
        "translation_batch_submitted",
        {
            "batch_id": data.get("id"),
            "status": data.get("status"),
            "input_file_id": input_file_id,
            "requests": len(requests),
            "retry_only_high": retry_only_high,
            "qa_retry_chunks": qa_retry_chunks,
            "strong_formatting_retries": strong_formatting_retries,
            "batch_requests_path": str(paths["batch_requests"]),
            "batch_map_path": str(paths["batch_map"]),
            "qa_snapshot": {
                **qa_snapshot_meta,
                "path": str(qa_snapshot_path),
            },
        },
    )
    print(
        json.dumps(
            {
                "requests": len(requests),
                "batch_requests": str(paths["batch_requests"]),
                "batch_id": data.get("id"),
                "status": data.get("status"),
                "input_file_id": input_file_id,
                "qa_snapshot_path": str(qa_snapshot_path),
                "qa_scope": qa_snapshot_meta.get("scope_type"),
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
    qa_scope = summarize_qa_scope(request_map.get("requests", []), all_files=visible_content_files(paths["source_dir"]))
    request_map["qa_scope"] = qa_scope

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
    append_iteration_event(
        project_dir,
        "qa_batch_submitted",
        {
            "batch_id": data.get("id"),
            "status": data.get("status"),
            "input_file_id": input_file_id,
            "requests": len(requests),
            "qa_batch_requests_path": str(paths["qa_batch_requests"]),
            "qa_batch_map_path": str(paths["qa_batch_map"]),
            "qa_scope": qa_scope,
        },
    )
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
    if should_use_qa_feedback(args):
        qa_snapshot_path, qa_snapshot_meta, qa_feedback = resolve_qa_snapshot(
            project_dir,
            chapter_list,
            explicit_snapshot=args.qa_snapshot,
            allow_partial_retry=args.allow_partial_qa_retry,
        )
    else:
        qa_snapshot_path = paths["qa_cloud"]
        qa_snapshot_meta = {"scope_type": "disabled", "hrefs": [], "chapter_count": 0, "chunk_count": 0}
        qa_feedback = {}
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
    log(
        f"[translate-direct] Starting. Chunks selected in scope: {total_selected} "
        f"(qa_scope={qa_snapshot_meta.get('scope_type')}, qa_snapshot={qa_snapshot_path})"
    )
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
                    config["translation"].get("source_language", "fr"),
                    config["translation"].get("target_language", "en"),
                    qa_feedback_text=qa_feedback_text,
                    strong_formatting_retry=strong_formatting_retry,
                ),
            }
            temperature = config["openai"].get("temperature")
            if config["openai"].get("send_temperature", True) and temperature is not None:
                body["temperature"] = temperature
            reasoning_effort = reasoned_effort_for_mode(config, "targeted_retry" if qa_feedback_text else "draft")
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


def cmd_validate_local(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    paths = project_paths(project_dir)
    max_chars = args.max_chars or config["translation"]["max_chars_per_chunk"]
    selected_files = [args.chapter] if args.chapter else visible_content_files(paths["source_dir"])
    source_language = config["translation"].get("source_language", "fr")
    target_language = config["translation"].get("target_language", "en")
    marker_tokens = source_language_profile(source_language).get("markers", set())
    report_lines = ["# Local QA", "", f"Project: `{project_dir.name}`", "", "## Findings", ""]
    finding_count = 0
    checked_chunks = 0

    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))
    for href in selected_files:
        source_file = paths["source_dir"] / href
        if not source_file.exists():
            continue
        _, source_units = collect_text_units(source_file)
        translated_lookup = progress_lookup_for_href(progress, href)
        chunks = chunk_units(source_units, max_chars)
        for chunk_index, chunk in enumerate(chunks):
            checked_chunks += 1
            issues: List[Dict[str, str]] = []
            for unit in chunk:
                source_text = unit.plain_text
                translation_text = render_unit_translation(unit, translated_lookup)
                if any("[[SEG_" in translated_lookup.get((target.xpath, target.field), "") for target in unit.targets):
                    issues.append(
                        {
                            "severity": "high",
                            "category": "formatting",
                            "source_excerpt": source_text,
                            "translation_excerpt": translation_text,
                            "note": "Unresolved placeholder token leaked into the translated XHTML.",
                        }
                    )
                mapped = structural_translation(source_text, source_language, target_language)
                if mapped and normalize_space(translation_text) and normalize_space(translation_text) != normalize_space(mapped):
                    issues.append(
                        {
                            "severity": "medium",
                            "category": "accuracy",
                            "source_excerpt": source_text,
                            "translation_excerpt": translation_text,
                            "note": "Structural/nav label differs from deterministic local translation mapping.",
                        }
                    )
                lowered = f" {translation_text.lower()} "
                if marker_tokens and sum(marker in lowered for marker in marker_tokens) >= 3:
                    issues.append(
                        {
                            "severity": "medium",
                            "category": "leftover_french",
                            "source_excerpt": source_text,
                            "translation_excerpt": translation_text,
                            "note": f"{source_language_name(source_language)} marker heuristic suggests untranslated source text remains in the target text.",
                        }
                    )
                if has_obvious_number_mismatch(source_text, translation_text):
                    severity = "high" if any(len(token) == 4 for token in extract_number_tokens(source_text)) else "medium"
                    issues.append(
                        {
                            "severity": severity,
                            "category": "accuracy",
                            "source_excerpt": source_text,
                            "translation_excerpt": translation_text,
                            "note": "Source and translation contain different number/year tokens.",
                        }
                    )
                if re.search(r"\s+[,.!?;:]", translation_text) or re.search(r"[A-Za-zÀ-ÿ]{6,}[A-ZÀ-ÖØ-Þ][A-Za-zÀ-ÿ]+", translation_text):
                    issues.append(
                        {
                            "severity": "medium",
                            "category": "formatting",
                            "source_excerpt": source_text,
                            "translation_excerpt": translation_text,
                            "note": "Local typography/spacing heuristic detected likely text damage.",
                        }
                    )
            if not issues:
                continue
            finding_count += len(issues)
            report_lines.append(f"### {href} chunk {chunk_index}")
            report_lines.append("- summary: Local deterministic checks found repairable risks.")
            for issue in issues:
                report_lines.append(f"- `{issue['severity']}` `{issue['category']}`: {issue['note']}")
                report_lines.append(f"- source: {issue['source_excerpt']}")
                report_lines.append(f"- translation: {issue['translation_excerpt']}")
            report_lines.append("")

    if finding_count == 0:
        report_lines.append("No issues detected by local validators.")
        report_lines.append("")
    report_lines.extend(["## Summary", f"- checked_chunks: `{checked_chunks}`", f"- findings: `{finding_count}`"])
    paths["qa_local"].write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "checked_chunks": checked_chunks,
                "findings": finding_count,
                "qa_local_report": str(paths["qa_local"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_build_remediation_plan(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    paths = project_paths(project_dir)
    snapshot_path, snapshot_meta = resolve_snapshot_for_manifest(project_dir, explicit_snapshot=args.qa_snapshot)
    manifest = build_remediation_manifest(project_dir, config, snapshot_path, snapshot_meta)
    output_path = Path(args.output) if args.output else paths["remediation_plan"]
    save_remediation_plan(output_path, manifest)
    append_iteration_event(
        project_dir,
        "remediation_plan_built",
        {
            "plan_path": str(output_path),
            "qa_snapshot": manifest["qa_snapshot"],
            "selected_high_chunks": manifest["summary"]["selected_high_chunks"],
        },
    )
    print(
        json.dumps(
            {
                "plan_path": str(output_path),
                "selected_high_chunks": manifest["summary"]["selected_high_chunks"],
                "local_chunks": manifest["summary"]["local_chunks"],
                "model_chunks": manifest["summary"]["model_chunks"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_apply_local_fixes(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    paths = project_paths(project_dir)
    manifest_path = Path(args.plan) if args.plan else paths["remediation_plan"]
    manifest = load_remediation_plan(manifest_path)
    verify_frozen_snapshot(manifest)
    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))
    max_chars = config["translation"]["max_chars_per_chunk"]
    target_language = config["translation"].get("target_language", "en")
    changed_files: List[Path] = []
    changed_chunk_keys: List[str] = []

    for chunk_entry in manifest.get("chunks", []):
        local_issues = [issue for issue in chunk_entry.get("issues", []) if issue.get("fix_mode") == "local"]
        if not local_issues:
            chunk_entry["local_status"] = "not_applicable"
            continue
        translated_lookup = progress_lookup_for_href(progress, chunk_entry["href"])
        chunk = load_chunk_from_source(paths, chunk_entry["href"], chunk_entry["chunk_index"], max_chars)
        chunk_changed = False
        for unit in chunk:
            current_text = render_unit_translation_with_placeholders(unit, translated_lookup)
            updated_text = apply_local_fixes_to_unit(unit, current_text, local_issues, source_language, target_language)
            if updated_text == current_text:
                continue
            split_parts = split_translated_text(unit, normalize_translation_text(updated_text))
            for target, piece in zip(unit.targets, split_parts):
                translated_lookup[(target.xpath, target.field)] = piece
            chunk_changed = True
        if chunk_changed:
            target_file = save_translated_lookup_for_href(paths, progress, chunk_entry["href"], translated_lookup)
            changed_files.append(target_file)
            changed_chunk_keys.append(chunk_entry["chunk_key"])
            chunk_entry["changed"] = True
            chunk_entry["local_status"] = "applied"
        else:
            chunk_entry["local_status"] = "noop"

    save_json(paths["progress"], progress)
    if changed_files:
        normalize_translated_package(paths["translated_dir"], target_language)
        sync_navigation_documents(paths["translated_dir"])
        rezip_epub(paths["translated_dir"], paths["output_epub"])
    save_remediation_plan(manifest_path, manifest)
    append_iteration_event(
        project_dir,
        "local_fixes_applied",
        {
            "plan_path": str(manifest_path),
            "changed_chunks": len(changed_chunk_keys),
            "changed_files": len({str(path) for path in changed_files}),
        },
    )
    print(
        json.dumps(
            {
                "plan_path": str(manifest_path),
                "changed_chunks": changed_chunk_keys,
                "changed_files": len({str(path) for path in changed_files}),
                "output_epub": str(paths["output_epub"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_retry_targeted(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    paths = project_paths(project_dir)
    manifest_path = Path(args.plan) if args.plan else paths["remediation_plan"]
    manifest = load_remediation_plan(manifest_path)
    verify_frozen_snapshot(manifest)
    if manifest.get("targeted_retry_rounds", 0) >= 1:
        raise RuntimeError("Targeted retry already executed once for this remediation plan. Build a new plan before retrying again.")
    model = config["openai"].get("model")
    if not model:
        raise RuntimeError("Set a model first with configure-openai.")

    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))
    glossary = load_glossary(paths["glossary"])
    max_chars = config["translation"]["max_chars_per_chunk"]
    max_chunks = args.max_chunks or manifest.get("targeted_retry_max_chunks") or config["pipeline"]["targeted_retry_max_chunks"]
    selected = []
    for chunk_entry in manifest.get("chunks", []):
        model_issues = [issue for issue in chunk_entry.get("issues", []) if issue.get("fix_mode") == "model"]
        if not model_issues:
            chunk_entry["model_status"] = "not_applicable"
            continue
        if chunk_entry.get("model_status") not in {"pending", "noop"}:
            continue
        failed_key = f"{project_dir.name}:{chunk_entry['href']}:chunk:{chunk_entry['chunk_index']:04d}"
        failed_entry = progress.get("failed", {}).get(failed_key)
        if failed_entry and failed_entry.get("reason") == "apply_validation_failed":
            chunk_entry["model_status"] = "blocked"
            chunk_entry["last_error"] = "Blocked by previous placeholder validation failure."
            continue
        selected.append(chunk_entry)
        if len(selected) >= max_chunks:
            break

    estimated_input_chars = 0
    estimated_output_chars = 0
    for chunk_entry in selected:
        chunk = load_chunk_from_source(paths, chunk_entry["href"], chunk_entry["chunk_index"], max_chars)
        estimated_input_chars += sum(len(unit.text) for unit in chunk) + 1200
        estimated_output_chars += sum(len(unit.plain_text) for unit in chunk)
    estimated_cost = estimate_direct_request_cost(model, estimated_input_chars, estimated_output_chars)
    if manifest.get("estimated_spend_usd", 0.0) + estimated_cost > float(manifest.get("api_budget_usd", config["pipeline"]["api_budget_usd"])):
        manifest["stop_loss_triggered"] = "budget_exceeded"
        save_remediation_plan(manifest_path, manifest)
        raise RuntimeError(
            f"Estimated targeted retry cost ${estimated_cost:.4f} would exceed the remediation budget."
        )

    changed_files: List[Path] = []
    retried_chunks: List[str] = []
    source_language = config["translation"].get("source_language", "fr")
    target_language = config["translation"].get("target_language", "en")
    for chunk_entry in selected:
        translated_lookup = progress_lookup_for_href(progress, chunk_entry["href"])
        chunk = load_chunk_from_source(paths, chunk_entry["href"], chunk_entry["chunk_index"], max_chars)
        chunk_feedback = {
            "summary": chunk_entry.get("summary") or "Targeted retry",
            "issues": [issue for issue in chunk_entry.get("issues", []) if issue.get("fix_mode") == "model"],
        }
        body = {
            "model": model,
            "input": build_chunk_payload(
                chunk,
                glossary,
                source_language,
                target_language,
                qa_feedback_text=build_qa_feedback_text(chunk_feedback),
                strong_formatting_retry=chunk_has_high_formatting_issue(chunk_feedback),
            ),
        }
        temperature = config["openai"].get("temperature")
        if config["openai"].get("send_temperature", True) and temperature is not None:
            body["temperature"] = temperature
        reasoning_effort = reasoned_effort_for_mode(config, "targeted_retry")
        if reasoning_effort:
            body["reasoning"] = {"effort": reasoning_effort}
        response = post_openai_responses(api_key, body)
        output_text = detect_output_text(response)
        translations = extract_translations_from_response(output_text, expected_count=len(chunk))
        try:
            for unit, translated_text in zip(chunk, translations):
                split_parts = split_translated_text(unit, normalize_translation_text(translated_text))
                for target, piece in zip(unit.targets, split_parts):
                    translated_lookup[(target.xpath, target.field)] = piece
        except RuntimeError as exc:
            chunk_entry["model_status"] = "blocked"
            chunk_entry["last_error"] = str(exc)
            manifest["stop_loss_triggered"] = "placeholder_failure"
            save_remediation_plan(manifest_path, manifest)
            raise
        target_file = save_translated_lookup_for_href(paths, progress, chunk_entry["href"], translated_lookup)
        changed_files.append(target_file)
        retried_chunks.append(chunk_entry["chunk_key"])
        chunk_entry["changed"] = True
        chunk_entry["model_status"] = "applied"
        chunk_entry["last_error"] = None
        progress["completed"].setdefault(chunk_entry["href"], [])
        if chunk_entry["chunk_index"] not in progress["completed"][chunk_entry["href"]]:
            progress["completed"][chunk_entry["href"]].append(chunk_entry["chunk_index"])

    manifest["targeted_retry_rounds"] = manifest.get("targeted_retry_rounds", 0) + (1 if retried_chunks else 0)
    manifest["estimated_spend_usd"] = round(manifest.get("estimated_spend_usd", 0.0) + estimated_cost, 4)
    save_json(paths["progress"], progress)
    if changed_files:
        normalize_translated_package(paths["translated_dir"], target_language)
        sync_navigation_documents(paths["translated_dir"])
        rezip_epub(paths["translated_dir"], paths["output_epub"])
    save_remediation_plan(manifest_path, manifest)
    append_iteration_event(
        project_dir,
        "targeted_retry_applied",
        {
            "plan_path": str(manifest_path),
            "retried_chunks": len(retried_chunks),
            "estimated_cost_usd": round(estimated_cost, 4),
        },
    )
    print(
        json.dumps(
            {
                "plan_path": str(manifest_path),
                "retried_chunks": retried_chunks,
                "estimated_cost_usd": round(estimated_cost, 4),
                "output_epub": str(paths["output_epub"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_qa_changed(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    config = read_project_config(project_dir)
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    paths = project_paths(project_dir)
    manifest_path = Path(args.plan) if args.plan else paths["remediation_plan"]
    manifest = load_remediation_plan(manifest_path)
    verify_frozen_snapshot(manifest)
    model = config["openai"].get("model")
    if not model:
        raise RuntimeError("Set a model first with configure-openai.")
    max_chars = config["translation"]["max_chars_per_chunk"]
    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))
    target_language = config["translation"].get("target_language", "en")

    report_lines = ["# Changed-Chunks QA", "", f"Project: `{project_dir.name}`", "", "## Findings", ""]
    checked_chunks = 0
    all_issues: List[Dict] = []
    remaining_high_chunks: List[str] = []
    for chunk_entry in manifest.get("chunks", []):
        chunk = load_chunk_from_source(paths, chunk_entry["href"], chunk_entry["chunk_index"], max_chars)
        translated_lookup = progress_lookup_for_href(progress, chunk_entry["href"])
        pairs = qa_pairs_for_chunk(chunk, translated_lookup)
        body = {
            "model": model,
            "input": build_qa_payload(
                pairs,
                config["translation"].get("source_language", "fr"),
                target_language,
            ),
        }
        reasoning_effort = reasoned_effort_for_mode(config, "qa")
        if reasoning_effort:
            body["reasoning"] = {"effort": reasoning_effort}
        response = post_openai_responses(api_key, body)
        data = extract_json_object_from_response(detect_output_text(response))
        issues = annotate_qa_issues_for_gate(data.get("issues") or [])
        summary = data.get("summary") or "No summary."
        checked_chunks += 1
        has_blocking_high = any(qa_issue_effective_severity(issue) == "high" for issue in issues)
        chunk_entry["qa_changed_status"] = "passed" if not has_blocking_high else "failed"
        chunk_entry["qa_changed_issues"] = issues
        if has_blocking_high:
            remaining_high_chunks.append(chunk_entry["chunk_key"])
        if issues:
            report_lines.append(f"### {chunk_entry['href']} chunk {chunk_entry['chunk_index']}")
            report_lines.append(f"- summary: {summary}")
            for issue in issues:
                report_lines.append(f"- `{issue.get('severity', 'unknown')}` `{issue.get('category', 'unknown')}`: {issue.get('note', '').strip()}")
                effective_severity = qa_issue_effective_severity(issue)
                if effective_severity != issue.get("severity", "unknown"):
                    report_lines.append(
                        f"- gate: treated as `{effective_severity}` for final gate ({issue.get('gate_reason', 'gate_adjusted')})"
                    )
                report_lines.append(f"- source: {normalize_space(issue.get('source_excerpt', ''))}")
                report_lines.append(f"- translation: {normalize_space(issue.get('translation_excerpt', ''))}")
                all_issues.append(issue)
            report_lines.append("")

    if not all_issues:
        report_lines.append("No issues detected in changed-chunks QA.")
        report_lines.append("")

    counts = qa_counts_from_issue_list(all_issues)
    report_lines.extend(
        [
            "## Summary",
            f"- checked_chunks: `{checked_chunks}`",
            f"- high: `{counts['high']}`",
            f"- medium: `{counts['medium']}`",
            f"- low: `{counts['low']}`",
        ]
    )
    paths["qa_changed"].write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    baseline_high = manifest.get("summary", {}).get("baseline_counts", {}).get("high", 0)
    manifest["qa_changed"] = {
        "checked_chunks": checked_chunks,
        "counts": counts,
        "report_path": str(paths["qa_changed"]),
        "remaining_high_chunks": remaining_high_chunks,
        "ran_at": utc_now_iso(),
    }
    if manifest.get("targeted_retry_rounds", 0) >= 1 and counts["high"] >= baseline_high:
        manifest["stop_loss_triggered"] = "unchanged_high_after_retry"
    save_remediation_plan(manifest_path, manifest)
    append_iteration_event(
        project_dir,
        "qa_changed_completed",
        {
            "plan_path": str(manifest_path),
            "checked_chunks": checked_chunks,
            "counts": counts,
            "remaining_high_chunks": remaining_high_chunks,
        },
    )
    print(
        json.dumps(
            {
                "checked_chunks": checked_chunks,
                "counts": counts,
                "remaining_high_chunks": remaining_high_chunks,
                "qa_changed_report": str(paths["qa_changed"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def cmd_final_gate(args: argparse.Namespace) -> None:
    project_dir = infer_project_dir(args.project, None, Path(args.project_root))
    paths = project_paths(project_dir)
    manifest_path = Path(args.plan) if args.plan else paths["remediation_plan"]
    manifest = load_remediation_plan(manifest_path)
    qa_changed = manifest.get("qa_changed")
    if not qa_changed:
        raise RuntimeError("Changed-chunks QA has not been run for this remediation plan yet.")
    max_high = manifest.get("quality_gate", {}).get("max_high", 0)
    high_count = qa_changed.get("counts", {}).get("high", 0)
    stop_loss = manifest.get("stop_loss_triggered")
    status = "PASS" if high_count <= max_high and not stop_loss else "FAIL"
    result = {
        "status": status,
        "max_high": max_high,
        "high": high_count,
        "medium": qa_changed.get("counts", {}).get("medium", 0),
        "low": qa_changed.get("counts", {}).get("low", 0),
        "remaining_high_chunks": qa_changed.get("remaining_high_chunks", []),
        "stop_loss_triggered": stop_loss,
        "qa_changed_report": qa_changed.get("report_path"),
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
    batch_map = load_json(paths["batch_map"], {"requests": []})
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
    append_iteration_event(
        project_dir,
        "translation_batch_submitted",
        {
            "batch_id": data.get("id"),
            "status": data.get("status"),
            "input_file_id": input_file_id,
            "requests": None,
            "retry_only_high": None,
            "qa_retry_chunks": None,
            "strong_formatting_retries": None,
            "batch_requests_path": str(paths["batch_requests"]),
            "batch_map_path": str(paths["batch_map"]),
            "qa_snapshot": batch_map.get("qa_snapshot_meta") or qa_snapshot_metadata(paths),
        },
    )
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
    skipped_requests: List[Dict[str, str]] = []
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
        request_failed = False
        for unit, translated_text in zip(units, translations):
            try:
                split_parts = split_translated_text(unit, normalize_translation_text(translated_text))
            except RuntimeError as exc:
                skipped_requests.append(
                    {
                        "custom_id": custom_id,
                        "href": href,
                        "chunk_index": str(request["chunk_index"]),
                        "xpath": unit.xpath,
                        "reason": str(exc),
                    }
                )
                request_failed = True
                break
            for target, piece in zip(unit.targets, split_parts):
                translated_map[(target.xpath, target.field)] = piece
        if request_failed:
            progress["failed"][custom_id] = {
                "reason": "apply_validation_failed",
                "href": href,
                "chunk_index": request["chunk_index"],
                "detail": skipped_requests[-1]["reason"],
            }
            continue
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
        generate_qa_report(
            changed_files,
            load_glossary(paths["glossary"]),
            paths["qa"],
            config["translation"].get("source_language", "fr"),
        )
    batch_state = load_json(project_dir / "batches" / "last_batch.json", {})
    append_iteration_event(
        project_dir,
        "translation_batch_applied",
        {
            "batch_id": batch_state.get("id"),
            "output_file_id": batch_state.get("output_file_id"),
            "updated_files": len(changed_files),
            "skipped_requests": len(skipped_requests),
            "output_epub": str(paths["output_epub"]),
            "batch_output_path": str(output_path),
            "qa_ran": run_qa,
            "progress_sha256": file_sha256(paths["progress"]),
        },
    )
    print(
        json.dumps(
            {
                "updated_files": len(changed_files),
                "skipped_requests": skipped_requests,
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
    active_snapshot = register_qa_snapshot(
        project_dir,
        history_path,
        batch_map.get("requests", []),
        checked_chunks=checked_chunks,
        findings=finding_count,
        failed_chunks=failed_chunks,
        scope_override=batch_map.get("qa_scope"),
        make_active=True,
    )
    qa_batch_state = load_json(paths["qa_last_batch"], {})
    append_iteration_event(
        project_dir,
        "qa_batch_applied",
        {
            "batch_id": qa_batch_state.get("id"),
            "output_file_id": qa_batch_state.get("output_file_id"),
            "checked_chunks": checked_chunks,
            "findings": finding_count,
            "failed_chunks": failed_chunks,
            "qa_report": str(paths["qa_cloud"]),
            "qa_history_snapshot": str(history_path),
            "qa_snapshot": active_snapshot,
        },
    )
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
    init_parser.add_argument("--source-language", default="fr")
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
    configure_parser.add_argument("--source-language")
    configure_parser.add_argument("--target-language")
    configure_parser.add_argument("--model")
    configure_parser.add_argument("--temperature", type=float)
    configure_parser.add_argument("--send-temperature", action=argparse.BooleanOptionalAction, default=None)
    configure_parser.add_argument("--reasoning-effort", choices=["none", "low", "medium", "high"])
    configure_parser.add_argument("--use-batch", action=argparse.BooleanOptionalAction, default=None)
    configure_parser.add_argument("--run-qa-after-apply", action=argparse.BooleanOptionalAction, default=None)
    configure_parser.set_defaults(func=cmd_configure_openai)

    draft_parser = subparsers.add_parser("draft", help="Run the default first-pass translation flow for one chapter or the whole book.")
    draft_parser.add_argument("--project", required=True)
    draft_parser.add_argument("--project-root", default="projects")
    draft_parser.add_argument("--chapter")
    draft_parser.add_argument("--max-chars", type=int)
    draft_parser.add_argument("--max-chunks", type=int, help="Only used when the project is configured for direct mode.")
    draft_parser.set_defaults(func=cmd_draft)

    review_parser = subparsers.add_parser("review", help="Run cheap local validation only.")
    review_parser.add_argument("--project", required=True)
    review_parser.add_argument("--project-root", default="projects")
    review_parser.add_argument("--chapter")
    review_parser.add_argument("--max-chars", type=int)
    review_parser.set_defaults(func=cmd_review)

    finalize_parser = subparsers.add_parser("finalize", help="Normalize translated files, sync navigation, and build the output EPUB.")
    finalize_parser.add_argument("--project", required=True)
    finalize_parser.add_argument("--project-root", default="projects")
    finalize_parser.set_defaults(func=cmd_finalize)

    estimate_parser = subparsers.add_parser("estimate-cost", help="Estimate translation cost for a configured project.")
    estimate_parser.add_argument("--project", required=True)
    estimate_parser.add_argument("--project-root", default="projects")
    estimate_parser.add_argument("--model")
    estimate_parser.set_defaults(func=cmd_estimate_cost)

    iteration_parser = subparsers.add_parser("iteration-status", help="Show recent QA/retry/apply iteration events for a project.")
    iteration_parser.add_argument("--project", required=True)
    iteration_parser.add_argument("--project-root", default="projects")
    iteration_parser.add_argument("--limit", type=int, default=10)
    iteration_parser.set_defaults(func=cmd_iteration_status)

    glossary_parser = subparsers.add_parser("suggest-glossary", help="Extract glossary candidates from the source EPUB into a per-project suggestions file.")
    glossary_parser.add_argument("--project", required=True)
    glossary_parser.add_argument("--project-root", default="projects")
    glossary_parser.add_argument("--chapter")
    glossary_parser.add_argument("--max-candidates", type=int, default=80)
    glossary_parser.set_defaults(func=cmd_suggest_glossary)

    local_validate_parser = subparsers.add_parser("validate-local", help="Run deterministic local validation before cloud QA.")
    local_validate_parser.add_argument("--project", required=True)
    local_validate_parser.add_argument("--project-root", default="projects")
    local_validate_parser.add_argument("--chapter")
    local_validate_parser.add_argument("--max-chars", type=int)
    local_validate_parser.set_defaults(func=cmd_validate_local)

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
    prepare_parser.add_argument("--reuse-qa-feedback", action="store_true", help="Reuse the frozen QA snapshot for retry preparation. Disabled by default for safe draft runs.")
    prepare_parser.add_argument("--retry-only-high", action="store_true", help="When reusing QA_cloud feedback, retry only chunks with at least one high-severity QA issue.")
    prepare_parser.add_argument("--qa-snapshot", help="Optional QA markdown snapshot to use instead of the active QA state.")
    prepare_parser.add_argument("--allow-partial-qa-retry", action="store_true", help="Allow a whole-book retry even when the selected QA snapshot covers only part of the book.")
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
    run_batch_parser.add_argument("--reuse-qa-feedback", action="store_true", help="Reuse the frozen QA snapshot for retry submission. Disabled by default for safe draft runs.")
    run_batch_parser.add_argument("--retry-only-high", action="store_true", help="When reusing QA_cloud feedback, retry only chunks with at least one high-severity QA issue.")
    run_batch_parser.add_argument("--qa-snapshot", help="Optional QA markdown snapshot to use instead of the active QA state.")
    run_batch_parser.add_argument("--allow-partial-qa-retry", action="store_true", help="Allow a whole-book retry even when the selected QA snapshot covers only part of the book.")
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

    remediation_parser = subparsers.add_parser("build-remediation-plan", help="Build a deterministic remediation manifest from a frozen QA snapshot.")
    remediation_parser.add_argument("--project", required=True)
    remediation_parser.add_argument("--project-root", default="projects")
    remediation_parser.add_argument("--qa-snapshot")
    remediation_parser.add_argument("--output")
    remediation_parser.set_defaults(func=cmd_build_remediation_plan)

    local_fix_parser = subparsers.add_parser("apply-local-fixes", help="Apply deterministic local fixes described by a remediation plan.")
    local_fix_parser.add_argument("--project", required=True)
    local_fix_parser.add_argument("--project-root", default="projects")
    local_fix_parser.add_argument("--plan")
    local_fix_parser.set_defaults(func=cmd_apply_local_fixes)

    targeted_retry_parser = subparsers.add_parser("retry-targeted", help="Retry only model-fix chunks from a remediation plan.")
    targeted_retry_parser.add_argument("--project", required=True)
    targeted_retry_parser.add_argument("--project-root", default="projects")
    targeted_retry_parser.add_argument("--plan")
    targeted_retry_parser.add_argument("--max-chunks", type=int)
    targeted_retry_parser.set_defaults(func=cmd_retry_targeted)

    qa_changed_parser = subparsers.add_parser("qa-changed", help="Run QA only for chunks tracked by a remediation plan.")
    qa_changed_parser.add_argument("--project", required=True)
    qa_changed_parser.add_argument("--project-root", default="projects")
    qa_changed_parser.add_argument("--plan")
    qa_changed_parser.set_defaults(func=cmd_qa_changed)

    final_gate_parser = subparsers.add_parser("final-gate", help="Evaluate the final quality gate for a remediation plan.")
    final_gate_parser.add_argument("--project", required=True)
    final_gate_parser.add_argument("--project-root", default="projects")
    final_gate_parser.add_argument("--plan")
    final_gate_parser.set_defaults(func=cmd_final_gate)

    direct_parser = subparsers.add_parser("translate-direct", help="Translate chunks directly via Responses API, for one chapter or the whole book.")
    direct_parser.add_argument("--project", required=True)
    direct_parser.add_argument("--project-root", default="projects")
    direct_parser.add_argument("--chapter")
    direct_parser.add_argument("--max-chunks", type=int, help="Optional global limit for testing. If omitted, translate all remaining chunks.")
    direct_parser.add_argument("--max-chars", type=int)
    direct_parser.add_argument("--reuse-qa-feedback", action="store_true", help="Reuse the frozen QA snapshot for retry translation. Disabled by default for safe draft runs.")
    direct_parser.add_argument("--retry-only-high", action="store_true", help="When reusing QA_cloud feedback, retry only chunks with at least one high-severity QA issue.")
    direct_parser.add_argument("--qa-snapshot", help="Optional QA markdown snapshot to use instead of the active QA state.")
    direct_parser.add_argument("--allow-partial-qa-retry", action="store_true", help="Allow a whole-book retry even when the selected QA snapshot covers only part of the book.")
    direct_parser.set_defaults(func=cmd_translate_direct)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
