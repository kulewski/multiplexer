#!/usr/bin/env python3
"""Checks that the code in the examples' walkthroughs is the code in the
examples. A walkthrough builds a file up piece by piece, in fenced blocks
whose info string names the file:

    ```python file=backend.py
    ...
    ```

For every walkthrough, the blocks naming one file, in document order,
must be exactly that file, so the walkthrough cannot drift from the code
it explains. Blocks without `file=` are free text. Paths are relative to
the walkthrough's directory.

A file whose beginning the walkthrough leaves out, the system rules a
rules file starts with for instance, says so on its first block:

    ```protobuf file=example.rules from="# The example's peers and messages."

The blocks must then be exactly the file from the first line that starts
with that text.

Usage: examples/check_walkthroughs.py     exits 1 with the first difference of each file
"""

import difflib
import glob
import os
import re
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FENCE = re.compile(r"^```(\S*)\s*(.*)$")


def blocks_of(walkthrough: str) -> tuple[dict[str, list[str]], dict[str, str]]:
    """The fenced blocks naming a file, as {path: [block text, ...]} in
    document order, and {path: text} for the files shown from a line on."""
    blocks: dict[str, list[str]] = {}
    starts: dict[str, str] = {}
    current: str | None = None
    lines: list[str] = []
    with open(walkthrough) as f:
        for line in f:
            line = line.rstrip("\n")
            match = FENCE.match(line)
            if current is None and match:
                attributes = dict(part.split("=", 1) for part in shlex.split(match.group(2)) if "=" in part)
                current = attributes.get("file", "")
                if current and "from" in attributes and current not in blocks:
                    starts[current] = attributes["from"]
                lines = []
            elif current is not None and line.startswith("```"):
                if current:
                    blocks.setdefault(current, []).append("".join(text + "\n" for text in lines))
                current = None
            elif current is not None:
                lines.append(line)
    return blocks, starts


def from_line(text: str, start: str) -> str | None:
    """`text` from its first line starting with `start` on; None when there is none."""
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if line.startswith(start):
            return "".join(lines[index:])
    return None


def main() -> int:
    """Every walkthrough against its files."""
    status = 0
    checked = 0
    for walkthrough in sorted(glob.glob(os.path.join(HERE, "*", "walkthrough.md"))):
        directory = os.path.dirname(walkthrough)
        blocks, starts = blocks_of(walkthrough)
        for path, pieces in blocks.items():
            checked += 1
            target = os.path.join(directory, path)
            if not os.path.exists(target):
                print(f"{os.path.relpath(walkthrough)}: names {path}, which does not exist")
                status = 1
                continue
            assembled = "".join(pieces)
            actual = open(target).read()
            if path in starts:
                actual = from_line(actual, starts[path])
                if actual is None:
                    print(f"{os.path.relpath(walkthrough)}: {path} has no line starting with {starts[path]!r}")
                    status = 1
                    continue
            if assembled != actual:
                status = 1
                print(f"{os.path.relpath(walkthrough)}: the blocks for {path} differ from the file:")
                diff = difflib.unified_diff(
                    actual.splitlines(), assembled.splitlines(), "the file", "the walkthrough", lineterm="", n=1
                )
                for line in list(diff)[:12]:
                    print("  " + line)
    if status == 0:
        print(f"walkthroughs: {checked} files are what their walkthroughs build")
    return status


if __name__ == "__main__":
    sys.exit(main())
