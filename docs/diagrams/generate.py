"""Writes the step-by-step protocol pages under docs/ from the pictures in
pages.py: `generate.py` regenerates them, `generate.py --check` exits 1 when
a page on disk differs from what would be generated. format.sh runs both.
"""

import os
import sys

DOCS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(DOCS, "diagrams"))

from pages import DIAGRAMS  # noqa: E402


def main(argv: list[str]) -> int:
    """Write every page, or with --check report the stale ones and exit 1."""
    check = "--check" in argv
    stale = []
    for d in DIAGRAMS:
        path = os.path.join(DOCS, d.file)
        text = d.markdown()
        current = open(path).read() if os.path.exists(path) else None
        if check:
            if current != text:
                stale.append(d.file)
        elif current != text:
            with open(path, "w") as f:
                f.write(text)
            print("wrote docs/" + d.file)
    if stale:
        print("stale, run docs/diagrams/generate.py:", ", ".join(stale))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
