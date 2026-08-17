#!/usr/bin/env python3
"""Validates every Mermaid block in the repository's Markdown.

Two checks. A fast one runs always: a `;` inside a sequenceDiagram statement
ends the statement in Mermaid, which is the mistake that is easiest to make
in prose-like labels, so it is reported outright. A full one renders each
block with mermaid-cli (`mmdc`, or `npx -p @mermaid-js/mermaid-cli mmdc`),
when either is installed; a block that fails to render is reported with its
file and line, and the renderer's message.

Usage: check_mermaid.py [--fast]     exits 1 on any failure
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKIP_DIRS = {".git", ".cache", "external", "compdb"}


def markdown_files() -> list[str]:
    """Every .md file in the repository, build output skipped."""
    found = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not d.startswith("bazel-")]
        found += [os.path.join(dirpath, f) for f in filenames if f.endswith(".md")]
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


def renderer() -> list[str] | None:
    """The mermaid-cli command, or None when it is not installed."""
    if shutil.which("mmdc"):
        return ["mmdc"]
    if shutil.which("npx"):
        return ["npx", "-y", "-p", "@mermaid-js/mermaid-cli", "mmdc"]
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


def main(argv: list[str]) -> int:
    """Check every block; print failures as file:line: message."""
    fast_only = "--fast" in argv
    blocks = [(path, line, source) for path in markdown_files() for line, source in mermaid_blocks(path)]
    failures = []
    for path, line, source in blocks:
        for problem in fast_problems(source):
            failures.append("%s:%d: %s" % (os.path.relpath(path, ROOT), line, problem))
    command = None if fast_only else renderer()
    if command:
        with ThreadPoolExecutor(max_workers=4) as pool:
            for (path, line, _), message in zip(blocks, pool.map(lambda b: render(command, b[2]), blocks)):
                if message:
                    failures.append("%s:%d: %s" % (os.path.relpath(path, ROOT), line, message))
    elif not fast_only:
        print("check_mermaid: no mermaid-cli (mmdc or npx) found; only the fast checks ran", file=sys.stderr)
    for failure in failures:
        print(failure)
    print("check_mermaid: %d blocks, %d problems" % (len(blocks), len(failures)))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
