"""Writes rust/mx-multiplexer/src/defaults.rs from multiplexer/defaults.h,
so that the Rust library has the timeouts and limits the multiplexer and
the C++ and Python libraries are built with; format.sh runs it, with
--check under its --check.

Every `static const TYPE NAME = VALUE;` of the header becomes a `pub const`
with the comment above it as its documentation; a constant with no comment
of its own shares the one of the constant before it, as the header's
comments read. An unsigned count or size becomes a usize, a uint64_t a u64,
and a float, seconds, a `Duration` in whole milliseconds; a negative float,
which means no deadline, becomes `None` of an `Option<Duration>`.
"""

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HEADER = os.path.join(ROOT, "multiplexer", "defaults.h")
OUTPUT = os.path.join(ROOT, "rust", "mx-multiplexer", "src", "defaults.rs")

CONSTANT = re.compile(r"static const (unsigned int|float|std::uint64_t) (\w+) = ([^;]+);")
SECONDS = ("_SECONDS", "_TIME", "_INTERVAL", "_TIMEOUT")

PREAMBLE = """\
//! The timeouts and limits of multiplexer/defaults.h, which the multiplexer
//! and the C++ and Python libraries are built with, generated from it by
//! rust/generate_defaults.py; do not edit, format.sh runs it. Seconds are
//! `Duration`s here, sizes and counts `usize`, and a negative timeout, no
//! deadline, is `None`. docs/semantics.md has the same table with context.

use std::time::Duration;"""


def rust_value(cxx_type: str, name: str, value: str) -> tuple[str, str]:
    """The Rust type and value of one constant of the header."""
    if not name.endswith(SECONDS):
        if cxx_type == "unsigned int":
            return "usize", value.strip()
        if cxx_type == "std::uint64_t":
            return "u64", re.sub(r"(\d+)ULL", r"\1", value.strip())
        raise ValueError(f"{name}: a {cxx_type} that is not seconds")
    seconds = float(value.strip().rstrip("f"))
    if seconds < 0:
        return "Option<Duration>", "None"
    milliseconds = round(seconds * 1000)
    if abs(milliseconds - seconds * 1000) > 1e-6:
        raise ValueError(f"{name}: {value} is not a whole number of milliseconds")
    if milliseconds % 1000 == 0:
        return "Duration", f"Duration::from_secs({milliseconds // 1000})"
    return "Duration", f"Duration::from_millis({milliseconds})"


def generate(header: str) -> str:
    """The text of defaults.rs for the text of defaults.h."""
    lines = header.splitlines()
    out = [PREAMBLE]
    commented = ""  # the constant the comment above belongs to
    pending: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("//"):
            pending.append(stripped[2:].strip())
            continue
        match = CONSTANT.match(stripped)
        if match:
            name = match.group(2)
            if pending:
                comment, commented, pending = pending, name, []
            elif commented:
                comment = [f"See [`{commented}`]."]
            else:
                raise ValueError(f"{name} has no comment")
            rust_type, value = rust_value(*match.groups())
            out.append("")
            out.extend(f"/// {text}".rstrip() for text in comment)
            out.append(f"pub const {name}: {rust_type} = {value};")
            continue
        pending, commented = [], ""
    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if the committed file is not what it would write")
    arguments = parser.parse_args()
    with open(HEADER) as file:
        text = generate(file.read())
    if arguments.check:
        with open(OUTPUT) as file:
            if file.read() != text:
                print(
                    f"{os.path.relpath(OUTPUT, ROOT)} is stale: run python3 rust/generate_defaults.py", file=sys.stderr
                )
                return 1
        return 0
    with open(OUTPUT, "w") as file:
        file.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
