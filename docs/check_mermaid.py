#!/usr/bin/env python3
"""Validates every Mermaid block in the repository's Markdown, and the
pictures committed as SVG next to their Mermaid source.

Two checks. A fast one runs always: a `;` inside a sequenceDiagram statement
ends the statement in Mermaid, which is the mistake that is easiest to make
in prose-like labels, so it is reported outright. A full one renders each
block with mermaid-cli (`mmdc`, or `npx -p @mermaid-js/mermaid-cli@VERSION
mmdc`), when either is installed; a block that fails to render is reported
with its file and line, and the renderer's message.

A picture is a `name.mmd` beside a `name.svg`, for a page that cannot hold
Mermaid itself: a notebook, which GitHub shows without rendering it, or the
PyPI page, which is the README unrendered. The full check renders every
source and fails when the committed SVG differs; `--write` renders them all
in place. Every block in README.md follows a `<!-- pypi: ![...](name.svg)
-->` line naming the picture make/setup.py puts in its place on the PyPI
page, and that picture's `name.mmd` must be the block, which the fast check
checks.

Usage: check_mermaid.py [--fast | --write]     exits 1 on any failure
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKIP_DIRS = {".git", ".cache", "external", "compdb", ".venv", ".tools", "node_modules"}
# Pinned, so that a committed picture compares with a fresh render of the
# same renderer; bump it and run --write together.
CLI = "@mermaid-js/mermaid-cli@12.0.0"
# Pictures: ids that do not change from one render to the next, labels as
# SVG text rather than HTML, which shows wherever an SVG is an image, and
# the classic look in a system font, which embeds no font of its own.
PICTURE_CONFIG = {
    "deterministicIds": True,
    "look": "classic",
    "fontFamily": "arial, sans-serif",
    "flowchart": {"htmlLabels": False},
}
# The line above a README block: the picture the PyPI page shows instead.
PYPI_PICTURE = re.compile(r"^<!-- pypi: !\[[^\]]+\]\(([^)\s]+)\.svg\) -->$")


def markdown_files() -> list[str]:
    """Every .md file in the repository, build output skipped."""
    found = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not d.startswith("bazel-")]
        found += [os.path.join(dirpath, f) for f in filenames if f.endswith(".md")]
    return sorted(found)


def picture_sources() -> list[str]:
    """Every .mmd file in the repository: a picture's source."""
    found = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not d.startswith("bazel-")]
        found += [os.path.join(dirpath, f) for f in filenames if f.endswith(".mmd")]
    return sorted(found)


def mermaid_blocks(path: str) -> list[tuple[int, str]]:
    """(line number, source) of every ```mermaid block in the file."""
    blocks = []
    lines = open(path).read().split("\n")
    inside = False
    start = 0
    current: list[str] = []
    for number, line in enumerate(lines, 1):
        if not inside and line.strip() == "```mermaid":
            inside, start, current = True, number, []
        elif inside and line.strip() == "```":
            inside = False
            blocks.append((start, "\n".join(current)))
        elif inside:
            current.append(line)
    return blocks


def fast_problems(source: str) -> list[str]:
    """Mistakes that need no renderer to spot."""
    problems = []
    if source.lstrip().startswith("sequenceDiagram"):
        for line in source.split("\n"):
            if ";" in line and not line.strip().startswith("%%"):
                problems.append("';' ends a Mermaid statement; use a comma: %s" % line.strip())
    return problems


def readme_problems() -> list[str]:
    """README.md's Mermaid blocks without their picture for PyPI above them."""
    path = os.path.join(ROOT, "README.md")
    lines = open(path).read().split("\n")
    problems = []
    for line, source in mermaid_blocks(path):
        marker = PYPI_PICTURE.match(lines[line - 2]) if line > 1 else None
        if marker is None:
            problems.append("README.md:%d: no `<!-- pypi: ![what it shows](picture.svg) -->` line above it" % line)
            continue
        picture = os.path.join(ROOT, marker.group(1) + ".mmd")
        if not os.path.exists(picture) or open(picture).read().rstrip("\n") != source.rstrip("\n"):
            problems.append(
                "README.md:%d: %s.mmd is not this block: copy it there, then docs/check_mermaid.py --write"
                % (line, marker.group(1))
            )
    return problems


def renderer() -> list[str] | None:
    """The mermaid-cli command, or None when it is not installed."""
    if shutil.which("npx"):
        return ["npx", "-y", "-p", CLI, "mmdc"]
    if shutil.which("mmdc"):
        return ["mmdc"]
    return None


def render(command: list[str], source: str) -> str:
    """Empty string if the block renders, else the renderer's complaint."""
    with tempfile.TemporaryDirectory() as directory:
        src = os.path.join(directory, "block.mmd")
        out = os.path.join(directory, "block.svg")
        open(src, "w").write(source)
        result = subprocess.run(command + ["-i", src, "-o", out, "-q"], capture_output=True, text=True, timeout=300)
        if result.returncode == 0 and os.path.exists(out):
            return ""
        return (result.stderr or result.stdout).strip().split("\n")[-1]


def render_picture(command: list[str], source: str, svg: str) -> str:
    """Render the picture `source` into `svg`; empty string, or the renderer's complaint."""
    with tempfile.TemporaryDirectory() as directory:
        config = os.path.join(directory, "config.json")
        with open(config, "w") as out:
            json.dump(PICTURE_CONFIG, out)
        result = subprocess.run(
            command + ["-i", source, "-o", svg, "-c", config, "-q"], capture_output=True, text=True, timeout=300
        )
    if result.returncode == 0 and os.path.exists(svg):
        return ""
    return (result.stderr or result.stdout).strip().split("\n")[-1]


def stale_picture(command: list[str], source: str) -> str:
    """Empty string when the committed SVG is what `source` renders to."""
    committed = source[: -len(".mmd")] + ".svg"
    if not os.path.exists(committed):
        return "no %s beside it: docs/check_mermaid.py --write" % os.path.basename(committed)
    with tempfile.TemporaryDirectory() as directory:
        fresh = os.path.join(directory, "fresh.svg")
        message = render_picture(command, source, fresh)
        if message:
            return message
        with open(fresh, "rb") as rendered, open(committed, "rb") as kept:
            if rendered.read() != kept.read():
                return "%s is not what this renders to: docs/check_mermaid.py --write" % os.path.basename(committed)
    return ""


def write_pictures(command: list[str] | None) -> int:
    """Render every picture's source into its SVG."""
    if command is None:
        print("check_mermaid: --write needs mermaid-cli (mmdc or npx)", file=sys.stderr)
        return 1
    failures = 0
    for source in picture_sources():
        message = render_picture(command, source, source[: -len(".mmd")] + ".svg")
        if message:
            failures += 1
            print("%s: %s" % (os.path.relpath(source, ROOT), message))
    return 1 if failures else 0


def main(argv: list[str]) -> int:
    """Check every block and picture; print failures as file:line: message."""
    if "--write" in argv:
        return write_pictures(renderer())
    fast_only = "--fast" in argv
    blocks = [(path, line, source) for path in markdown_files() for line, source in mermaid_blocks(path)]
    blocks += [(path, 1, open(path).read()) for path in picture_sources()]
    failures = readme_problems()
    for path, line, source in blocks:
        for problem in fast_problems(source):
            failures.append("%s:%d: %s" % (os.path.relpath(path, ROOT), line, problem))
    command = None if fast_only else renderer()
    if command:
        with ThreadPoolExecutor(max_workers=4) as pool:
            for (path, line, _), message in zip(blocks, pool.map(lambda b: render(command, b[2]), blocks), strict=True):
                if message:
                    failures.append("%s:%d: %s" % (os.path.relpath(path, ROOT), line, message))
            sources = picture_sources()
            for path, message in zip(
                sources, pool.map(lambda source: stale_picture(command, source), sources), strict=True
            ):
                if message:
                    failures.append("%s:1: %s" % (os.path.relpath(path, ROOT), message))
    elif not fast_only:
        print("check_mermaid: no mermaid-cli (mmdc or npx) found; only the fast checks ran", file=sys.stderr)
    for failure in failures:
        print(failure)
    print("check_mermaid: %d blocks, %d problems" % (len(blocks), len(failures)))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
