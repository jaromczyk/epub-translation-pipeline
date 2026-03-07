#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
        "qa": project_dir / "QA.md",
        "qa_cloud": project_dir / "QA_cloud.md",
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
        if strip_ns(elem.tag) in IGNORE_TAGS:
            continue
        xpath = node_xpath(elem, parents)
        buffered_parts: List[TextTarget] = []
        if elem.text and normalize_space(elem.text):
            buffered_parts.append(TextTarget(xpath=xpath, field="text", original_text=elem.text))
        for idx, child in enumerate(list(elem), start=1):
            if child.tail and normalize_space(child.tail):
                child_tag = strip_ns(child.tag)
                is_pagebreak = child_tag == "span" and child.attrib.get(f"{{{EPUB_NS}}}type") == "pagebreak"
                target = TextTarget(xpath=f"{xpath}/tail[{idx}]", field="tail", original_text=child.tail)
                if is_pagebreak:
                    buffered_parts.append(target)
                    continue
                if buffered_parts:
                    unit_text = "".join(part.original_text for part in buffered_parts)
                    units.append(
                        TextUnit(
                            xpath=buffered_parts[0].xpath,
                            field=buffered_parts[0].field,
                            text=unit_text,
                            targets=buffered_parts.copy(),
                        )
                    )
                    buffered_parts.clear()
                units.append(
                    TextUnit(
                        xpath=target.xpath,
                        field=target.field,
                        text=target.original_text,
                        targets=[target],
                    )
                )
        if buffered_parts:
            unit_text = "".join(part.original_text for part in buffered_parts)
            units.append(
                TextUnit(
                    xpath=buffered_parts[0].xpath,
                    field=buffered_parts[0].field,
                    text=unit_text,
                    targets=buffered_parts.copy(),
                )
            )
    return tree, units


def chunk_units(units: List[TextUnit], max_chars: int) -> List[List[TextUnit]]:
    chunks: List[List[TextUnit]] = []
    current: List[TextUnit] = []
    current_chars = 0
    for unit in units:
        unit_chars = len(normalize_space(unit.text))
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

    source_parts = [normalize_space(target.original_text) for target in unit.targets]
    total = sum(max(len(part), 1) for part in source_parts)
    remaining = translated_text
    pieces: List[str] = []
    for index, target in enumerate(unit.targets):
        if index == len(unit.targets) - 1:
            pieces.append(preserve_outer_whitespace(target.original_text, remaining))
            break
        ratio = max(len(source_parts[index]), 1) / total
        split_at = max(1, min(len(remaining) - 1, round(len(remaining) * ratio)))
        while split_at < len(remaining) and not remaining[split_at].isspace():
            split_at += 1
        if split_at >= len(remaining):
            split_at = max(1, round(len(remaining) * ratio))
            while split_at > 1 and not remaining[split_at - 1].isspace():
                split_at -= 1
        piece = remaining[:split_at].rstrip()
        remaining = remaining[split_at:].lstrip()
        pieces.append(preserve_outer_whitespace(target.original_text, piece))
        total -= max(len(source_parts[index]), 1)
    return pieces


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
        translated_text = translated_lookup.get((unit.xpath, unit.field), "")
        pairs.append(
            {
                "id": str(index),
                "source": normalize_space(unit.text),
                "translation": normalize_space(translated_text),
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
        translated_lookup = {(unit.xpath, unit.field): unit.text for unit in translated_units}
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


def build_chunk_payload(chunk: List[TextUnit], glossary: Dict[str, str], target_language: str) -> List[Dict]:
    language_name = target_language_name(target_language)
    glossary_lines = [f"- {fr} => {en}" for fr, en in glossary.items()]
    glossary_text = "\n".join(glossary_lines) if glossary_lines else "- none yet"
    units = [{"id": index, "text": normalize_space(unit.text)} for index, unit in enumerate(chunk)]
    user_prompt = (
        f"Translate the following French text segments into {language_name}.\n"
        "Return strict JSON in the form {\"translations\": [{\"id\": 0, \"text\": \"...\"}]}.\n"
        "Do not omit any segment. Do not add commentary.\n"
        "Preserve the visible meaning, tone, and rhetorical structure.\n"
        f"Do not leave stray French words in the {language_name} unless they are proper nouns, publication titles, or intentionally retained foreign terms.\n"
        f"Avoid awkward literalism. Prefer idiomatic literary {language_name} where needed.\n"
        "Do not introduce doubled articles or doubled words.\n"
        "If you retain an italicized foreign term, inflect or frame the surrounding phrase so the sentence remains grammatically natural.\n"
        f"If a phrase is idiomatic or culturally embedded, render its meaning in fluent literary {language_name} rather than calquing it.\n"
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
        text = normalize_space(" ".join(unit.text for unit in units))
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
        text = normalize_space(" ".join(unit.text for unit in units))
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

    requests: List[str] = []
    request_map = {"project": project_dir.name, "requests": []}
    max_chars = args.max_chars or config["translation"]["max_chars_per_chunk"]
    for href in selected_files:
        _, units = collect_text_units(source_dir / href)
        chunks = chunk_units(units, max_chars)
        completed = set(progress["completed"].get(href, []))
        for chunk_index, chunk in enumerate(chunks):
            if chunk_index in completed:
                continue
            custom_id = f"{project_dir.name}:{href}:chunk:{chunk_index:04d}"
            body = {
                "model": model,
                "input": build_chunk_payload(chunk, glossary, config["translation"].get("target_language", "en")),
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
    progress = load_json(paths["progress"], create_progress_stub(Path(config["book"]["source_epub"])))
    selected_files = [args.chapter] if args.chapter else visible_content_files(source_dir)
    max_chars = args.max_chars or config["translation"]["max_chars_per_chunk"]

    requests: List[str] = []
    request_map = {"project": project_dir.name, "requests": []}
    for href in selected_files:
        _, units = collect_text_units(source_dir / href)
        chunks = chunk_units(units, max_chars)
        completed = set(progress["completed"].get(href, []))
        for chunk_index, chunk in enumerate(chunks):
            if chunk_index in completed:
                continue
            custom_id = f"{project_dir.name}:{href}:chunk:{chunk_index:04d}"
            body = {
                "model": model,
                "input": build_chunk_payload(chunk, glossary, config["translation"].get("target_language", "en")),
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
                    "units": [asdict(unit) for unit in chunk],
                }
            )

    ensure_dir(paths["batch_requests"].parent)
    paths["batch_requests"].write_text("\n".join(requests) + ("\n" if requests else ""), encoding="utf-8")
    save_json(paths["batch_map"], request_map)
    log(f"[run-batch] Prepared {len(requests)} request(s) at {paths['batch_requests']}")

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
    remaining_budget = args.max_chunks
    translated_summary = []
    total_remaining = 0
    for href in chapter_list:
        source_file = source_dir / href
        if not source_file.exists():
            continue
        _, units = collect_text_units(source_file)
        chunks = chunk_units(units, args.max_chars or config["translation"]["max_chars_per_chunk"])
        completed_set = set(progress["completed"].get(href, []))
        total_remaining += len([idx for idx in range(len(chunks)) if idx not in completed_set])
    log(f"[translate-direct] Starting. Remaining chunks in scope: {total_remaining}")
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
        available_indexes = [idx for idx in range(len(chunks)) if idx not in completed_set]
        if not available_indexes:
            continue
        log(f"[translate-direct] Chapter {href}: remaining chunks {len(available_indexes)} / total {len(chunks)}")
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
                f"({len(chunk)} segments, approx_chars={sum(len(normalize_space(u.text)) for u in chunk)})"
            )
            body = {
                "model": model,
                "input": build_chunk_payload(chunk, glossary, config["translation"].get("target_language", "en")),
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
    paths["qa_cloud"].write_text("\n".join(report_lines), encoding="utf-8")
    print(
        json.dumps(
            {
                "checked_chunks": checked_chunks,
                "findings": finding_count,
                "failed_chunks": failed_chunks,
                "qa_report": str(paths["qa_cloud"]),
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
    direct_parser.set_defaults(func=cmd_translate_direct)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
