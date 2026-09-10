#!/usr/bin/env python3
"""Generates docs/code_map.md: one line per source file, taken from the
file's own header comment, plus an index of where each mechanism lives.

The header comment is the first comment block of the file (// lines in C++
and .proto, the module docstring in Python and .bzl, # lines in BUILD and
shell files); its first paragraph is the file's line in the map. So the map
cannot drift from the code: edit the header comment, run ./format.sh, and
./format.sh --check fails when the map is stale.

Usage: code_map.py [--check]
"""

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "docs", "code_map.md")
SCENARIOS_OUT = os.path.join(ROOT, "tests", "scenarios", "README.md")

# Directories in reading order, with a sentence each.
SECTIONS = [
    ("multiplexer", "The multiplexer and both client libraries"),
    ("multiplexer/io", "Frames and connections"),
    ("multiplexer/backend", "The C++ backend base class"),
    ("multiplexer/mxlog", "Python logging"),
    ("multiplexer/util", "Python helpers"),
    ("multiplexer/testing", "Test infrastructure, importable as multiplexer.testing"),
    ("mxcontrol", "The command-line tool"),
    ("lib", "The C++ base library (namespace mx)"),
    ("lib/logging", "C++ logging"),
    ("lib/preproc", "Preprocessor helpers"),
    ("lib/encoding", "Byte encoders"),
    ("lib/protobuf", "Protocol buffer streams"),
    ("tests", "Integration tests"),
    ("tests/harness", "The harness of this repository's tests: multiplexer.testing plus its constants"),
    ("tests/roles/py", "Test roles in Python"),
    ("tests/roles/cc", "Test roles in C++"),
    ("tests/scenarios", "Scenarios"),
    ("examples/echo", "The echo example"),
    ("bazel", "Build machinery"),
    ("docker", "Clean-machine checks"),
    ("make", "The build without Bazel"),
    ("docs", "Documentation generators"),
    ("docs/diagrams", "The step-by-step pictures"),
    (".", "Repository root"),
]

# Where each mechanism lives: the questions a reader asks first.
INDEX = [
    ("Routing a message by its type, `to` or override rules", "multiplexer/server.h", "`Server::_handle_message`"),
    ("Round robin for `whom: ANY`", "multiplexer/server.h", "`Server::send_to_one`"),
    ("Delivery errors", "multiplexer/server.h", "`Server::_handle_delivery_errors`"),
    ("The backend search a query falls back to", "multiplexer/server.h", "`Server::_handle_meta_message`"),
    (
        "The query algorithm, C++ and Python",
        "multiplexer/client.h",
        "`Client::_query`; `Client.query` in `multiplexer/mxclient.py`",
    ),
    (
        "A client with its own io thread, queries in flight at once",
        "multiplexer/threaded_client.h",
        "`ThreadedClient`; `multiplexer/threaded_client.py` for Python",
    ),
    ("Frame layout, size limit, CRC", "multiplexer/io/raw_message.h", "`RawMessage`"),
    ("The welcome handshake", "multiplexer/io/connection.h", "`Connection::_receive_internal_message`"),
    (
        "Heartbeats and dropping silent peers",
        "multiplexer/io/connection.h",
        "`Connection::_send_heartbit_now`, `_require_heartbit_soon`",
    ),
    ("The passive-peer exemption", "multiplexer/io/connection.h", "`Connection::set_is_passive`"),
    (
        "Registering a peer, id clashes",
        "multiplexer/connections_manager.h",
        "`ConnectionsManager::register_connection`",
    ),
    ("Per-connection queues and dropping when full", "multiplexer/io/connection.h", "`Connection::schedule`"),
    ("Reconnecting after a connection drops", "multiplexer/basic_client.cc", "`BasicClient::connection_destroyed`"),
    ("Dropping duplicate messages by id", "multiplexer/basic_client.cc", "`BasicClient::handle_message`"),
    (
        "What a backend does with a search or a PING",
        "multiplexer/backend/base_multiplexer_server.cc",
        "`__handle_internal_message`; same in `multiplexer/clients.py`",
    ),
    (
        "What happens when a handler throws",
        "multiplexer/backend/base_multiplexer_server.cc",
        "`__handle_message`; same in `multiplexer/servers.py`",
    ),
    (
        "Draining a backend before it exits",
        "multiplexer/backend/base_multiplexer_server.cc",
        "`serve_forever`, `periodic_task`, `start_draining`, `drained`; the same names in `multiplexer/servers.py`",
    ),
    ("Reading the rules file", "multiplexer/config.h", "`Config::read_configuration`"),
    (
        "Generating the constants from the rules file",
        "multiplexer/generate_constants.cc",
        "`write_cxx`, `write_python`",
    ),
    ("Every timeout and limit", "multiplexer/defaults.h", ""),
    ("The Python binding", "multiplexer/_native.cc", "`PYBIND11_MODULE`"),
    (
        "Starting a multiplexer, signals, the port file",
        "mxcontrol/start_multiplexer_server.cc",
        "`StartMultiplexerServer::run`",
    ),
    ("Registering an mxcontrol subcommand", "mxcontrol/tasks_holder.h", "`REGISTER_MXCONTROL_SUBCOMMAND`"),
    (
        "Log entries and the binary log stream",
        "lib/logging/logging.h",
        "`MX_LOG`; `lib/protobuf/stream.h` for the stream",
    ),
    ("Starting multiplexers and roles in a test", "multiplexer/testing/__init__.py", "`Cluster`, `spawn`"),
    ("Scripted peers in-process, for unit tests", "multiplexer/testing/fakes.py", "`FakePeer`, `BackendThread`"),
    ("A peer without the library, for protocol tests", "multiplexer/testing/raw_peer.py", "`RawPeer`"),
    ("Recording routed messages", "multiplexer/recorder.h", "`Recorder`; `multiplexer/recording.py` reads"),
    (
        "Recording sessions and taps asked for over the protocol",
        "multiplexer/server.cc",
        "`Server::_handle_recording_control`; `mxcontrol/recording_control.cc` asks",
    ),
    (
        "Thread-safety annotations and the wrong-thread check",
        "lib/thread_checker.h",
        "`MX_DCHECK_RUN_ON`; `lib/thread_annotations.h`, `lib/mutex.h`",
    ),
    ("Stripping release binaries", "bazel/maybe_strip.bzl", "`maybe_strip_cc_binary`"),
    ("Consuming this repository as `@mx`", "bazel/deps.bzl", "`mx_dependencies`, `mx_setup` in `bazel/setup.bzl`"),
]

SKIP_DIRS = {".git", "bazel-bin", "bazel-out", "bazel-testlogs", "__pycache__", ".cache", "compdb", "build"}
SKIP_FILES = {"compile_commands.json", "external"}


def source_files():
    """Every source file under the repository, relative paths in walk order,
    skipping build output and the docs pages themselves."""
    for dirpath, dirnames, filenames in os.walk(ROOT):
        rel = os.path.relpath(dirpath, ROOT)
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS and not d.startswith("bazel-"))
        if rel.startswith("examples/echo/bazel") or "/bazel-" in rel:
            continue
        for name in sorted(filenames):
            if name in SKIP_FILES:
                continue
            ext = os.path.splitext(name)[1]
            if ext in (".h", ".cc", ".py", ".bzl", ".proto", ".sh") or name in ("BUILD", "WORKSPACE", ".bazelrc"):
                yield os.path.normpath(os.path.join(rel, name))


def header_comment(path: str) -> str:
    """The first paragraph of the file's leading comment, as one string."""
    with open(os.path.join(ROOT, path), encoding="utf-8", errors="replace") as f:
        text = f.read()
    ext = os.path.splitext(path)[1]
    name = os.path.basename(path)
    if ext in (".py", ".bzl"):
        m = re.match(r'\s*(?:#![^\n]*\n)?\s*(?:#[^\n]*\n\s*)*"""(.*?)"""', text, re.S)
        if not m:
            return ""
        return first_paragraph(m.group(1).strip().splitlines())
    if ext in (".h", ".cc", ".proto"):
        lines = []
        for line in text.splitlines():
            if line.startswith("//"):
                lines.append(line[2:].strip())
            elif line.strip() == "" and not lines:
                continue
            else:
                break
        return first_paragraph(lines)
    # BUILD, WORKSPACE, .bazelrc, .sh: leading # lines, shebang skipped
    lines = []
    for line in text.splitlines():
        if line.startswith("#!"):
            continue
        if line.startswith("#"):
            lines.append(line[1:].strip())
        elif line.strip() == "" and not lines:
            continue
        else:
            break
    return first_paragraph(lines)


def first_paragraph(lines: list[str]) -> str:
    """The first non-empty run of lines, joined into one string."""
    out = []
    for line in lines:
        if not line.strip():
            if out:
                break
            continue
        out.append(line.strip())
    return " ".join(out)


def render() -> str:
    """The whole code map as Markdown."""
    by_dir = {}
    for path in source_files():
        d = os.path.dirname(path) or "."
        by_dir.setdefault(d, []).append(path)
    parts = [
        "# Code map\n",
        "Generated by `docs/code_map.py` from each file's header comment; do not edit by hand. "
        "One line per file, then an index of where each mechanism lives. "
        "The vocabulary is the one in [README.md](README.md).\n",
        "## Where things happen\n",
        "| Looking for | File | Where in it |",
        "|---|---|---|",
    ]
    for what, path, where in INDEX:
        parts.append("| %s | [%s](../%s) | %s |" % (what, path, path, where))
    parts.append("")
    parts.append("## Files\n")
    listed = set()
    for d, title in SECTIONS:
        files = by_dir.get(d)
        if not files:
            continue
        parts.append("### %s: %s\n" % (d if d != "." else "root", title))
        for path in files:
            listed.add(path)
            summary = header_comment(path) or "(no header comment)"
            parts.append("- [%s](../%s): %s" % (os.path.basename(path), path, summary))
        parts.append("")
    rest = sorted(set(p for ps in by_dir.values() for p in ps) - listed)
    if rest:
        parts.append("### Elsewhere\n")
        for path in rest:
            parts.append("- [%s](../%s): %s" % (path, path, header_comment(path) or "(no header comment)"))
        parts.append("")
    return "\n".join(parts)


def render_scenarios() -> str:
    """The index of the integration scenarios: one line per scenario folder,
    from the scenario's module docstring, linking to its README."""
    root = os.path.join(ROOT, "tests", "scenarios")
    parts = [
        "# Scenarios\n",
        "Generated by `docs/code_map.py` from each scenario's docstring; do not edit by hand. "
        "Every scenario has a folder with the test and a README that draws what happens and "
        "lists what is checked. `bazel test //tests/scenarios:all` runs them; "
        "the ones tagged slow wait out real heartbeat intervals.\n",
    ]
    for name in sorted(os.listdir(root)):
        script = os.path.join(root, name, name + ".py")
        if not os.path.isfile(script):
            continue
        summary = header_comment(os.path.relpath(script, ROOT))
        title = name.replace("_", " ")
        parts.append("- [%s](%s/README.md): %s" % (title[0].upper() + title[1:], name, summary))
    parts.append("")
    return "\n".join(parts)


def main(argv: list[str]) -> int:
    """Write the map and the scenario index, or with --check compare them to
    the files on disk."""
    outputs = [(OUT, render()), (SCENARIOS_OUT, render_scenarios())]
    if "--check" in argv:
        stale = [path for path, text in outputs if (open(path).read() if os.path.exists(path) else "") != text]
        if stale:
            sys.stderr.write("stale, run ./format.sh: %s\n" % ", ".join(os.path.relpath(p, ROOT) for p in stale))
            return 1
        return 0
    for path, text in outputs:
        with open(path, "w") as out:
            out.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
