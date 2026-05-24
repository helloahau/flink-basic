#!/usr/bin/env python3
"""Extract Java code from 11-flink Yuque pages into this Maven project."""

from __future__ import annotations

import json
import re
import subprocess
import urllib.parse
from pathlib import Path

BOOK_URL = "https://www.yuque.com/yxiansheng-njx6f/vz4qyd"
BOOK_ID = 69186743
LEARN = Path("/Volumes/Work/Flink/Learn")
OUT = Path("/Volumes/Work/Flink/Project/basic")
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

FLINK_SLUGS = [
    ("og6x2xwcgh475ixn", "flink常见面试题"),
    ("ghbsuc1vk9bddm0f", "面试准备"),
    ("nuykp00iwcgic2qa", "Windows版本的kafka的搭建与使用"),
    ("wiq1t6e6i5on9bgi", "大数据技术之Flink优化"),
    ("iqbixxys9icdza0s", "Day06-Flink-高级部分"),
    ("pb9wqs8sv708mmpl", "Day05-FlinkSQL"),
    ("ga1gssxnquebp8co", "Day04-Flink四大基石2"),
    ("cam5acsuc1c9pehx", "Day03-Flink四大基石"),
    ("sanl8zvppg33tu3u", "eventTime-watermarker"),
    ("ves1cik0ui24swqt", "Day02-Flink-普通API的使用"),
    ("or26ohn31g01xmmy", "Day01-Flink-安装与入门"),
]


def curl_get(url: str, referer: str) -> str:
    return subprocess.check_output(
        [
            "curl", "-sS", "-L", "--max-time", "120",
            "-H", f"User-Agent: {UA}",
            "-H", f"Referer: {referer}",
            url,
        ],
        text=True,
    )


def decode_card(raw: str) -> dict:
    val = urllib.parse.unquote(raw)
    if val.startswith("data:"):
        val = val[5:]
    return json.loads(val)


def classify(code: str, mode: str) -> str:
    if mode == "java":
        if code.strip().startswith("<"):
            return "xml"
        if "log4j." in code and "appender" in code:
            return "properties"
        if re.search(r"\.bat\b|\.sh\b|cd D:|dataDir=", code):
            return "shell"
    if re.search(r"^\s*package\s+[\w.]+;", code, re.M) and re.search(
        r"\b(class|interface|enum)\s+\w+", code
    ):
        return "java-class"
    if re.search(r"\bpublic\s+class\s+\w+", code):
        return "java-class"
    if mode == "java":
        return "java-snippet"
    return "skip"


def parse_class(code: str) -> tuple[str | None, str | None]:
    pkg = re.search(r"package\s+([\w.]+)\s*;", code)
    cls = re.search(r"(?:public\s+)?(?:class|interface|enum)\s+(\w+)", code)
    return (pkg.group(1) if pkg else None, cls.group(1) if cls else None)


def safe_slug(name: str) -> str:
    return re.sub(r"[^\w\-]+", "_", name)[:60]


def wrap_snippet(code: str, slug: str, title: str, index: int) -> str:
    header = f"// Source: {title} ({slug}) snippet #{index}\n"
    stripped = code.strip()
    if re.search(r"\b(class|interface|enum)\s+\w+", stripped):
        if not stripped.startswith("package "):
            return header + "package com.bigdata.snippets;\n\n" + stripped + "\n"
        return header + stripped + "\n"
    return (
        header
        + "package com.bigdata.snippets;\n\n"
        + f"public class {safe_slug(slug)}Snippet{index:03d} {{\n"
        + "    // Tutorial snippet — may require surrounding context to compile.\n"
        + "    public static void demo() throws Exception {\n"
        + "\n".join("        " + line if line.strip() else "" for line in stripped.splitlines())
        + "\n    }\n}\n"
    )


def unique_path(base: Path) -> Path:
    if not base.exists():
        return base
    stem, suffix = base.stem, base.suffix
    n = 2
    while True:
        candidate = base.with_name(f"{stem}_{n}{suffix}")
        if not candidate.exists():
            return candidate
        n += 1


def main() -> None:
    java_root = OUT / "src" / "main" / "java"
    res_root = OUT / "src" / "main" / "resources"
    snippet_root = java_root / "com" / "bigdata" / "snippets"
    meta: list[dict] = []

    for d in [java_root, res_root, snippet_root]:
        d.mkdir(parents=True, exist_ok=True)

    saved_classes = 0
    saved_snippets = 0
    saved_resources = 0

    for slug, title in FLINK_SLUGS:
        url = f"https://www.yuque.com/api/docs/{slug}?book_id={BOOK_ID}&merge_dynamic_data=false"
        data = json.loads(curl_get(url, f"{BOOK_URL}/{slug}"))["data"]
        content = data.get("content", "")
        snippet_idx = 0

        for match in re.finditer(
            r'<card[^>]*name="codeblock"[^>]*value="([^"]+)"[^>]*>\s*</card>',
            content,
            re.I,
        ):
            card = decode_card(match.group(1))
            mode = (card.get("mode") or "plain").lower()
            code = card.get("code") or ""
            if mode not in ("java", "plain"):
                continue
            if mode == "plain" and not re.search(r"\b(public|class|import|package|<)\b", code):
                continue

            kind = classify(code, mode)
            entry = {"doc": title, "slug": slug, "kind": kind, "file": None}

            if kind == "java-class":
                pkg, cls = parse_class(code)
                if cls:
                    pkg_path = Path(*pkg.split(".")) if pkg else Path("com/bigdata")
                    dest = unique_path(java_root / pkg_path / f"{cls}.java")
                else:
                    dest = unique_path(snippet_root / safe_slug(slug) / f"Block_{snippet_idx:03d}.java")
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text(code.strip() + "\n", encoding="utf-8")
                entry["file"] = str(dest.relative_to(OUT))
                saved_classes += 1
            elif kind == "java-snippet":
                snippet_idx += 1
                dest = snippet_root / safe_slug(slug) / f"Snippet_{snippet_idx:03d}.java"
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text(wrap_snippet(code, slug, title, snippet_idx), encoding="utf-8")
                entry["file"] = str(dest.relative_to(OUT))
                saved_snippets += 1
            elif kind in ("xml", "properties", "shell"):
                ext = {"xml": ".xml", "properties": ".properties", "shell": ".txt"}[kind]
                dest = res_root / safe_slug(slug) / f"block_{snippet_idx:03d}{ext}"
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text(code.strip() + "\n", encoding="utf-8")
                entry["file"] = str(dest.relative_to(OUT))
                saved_resources += 1
                snippet_idx += 1
            else:
                continue

            meta.append(entry)

    (OUT / "sources.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Classes: {saved_classes}, snippets: {saved_snippets}, resources: {saved_resources}")


if __name__ == "__main__":
    main()
