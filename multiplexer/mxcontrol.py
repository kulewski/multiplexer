"""The mxcontrol that came with this package: a pip install carries it in
multiplexer/bin as package data, and Bazel's runfiles hold //mxcontrol next
to the package. `binary_path()` names it, for programs, supervisors and the
test harness; `main()`, the `mxcontrol` command pip installs and `python -m
multiplexer.mxcontrol`, runs it in this process's place, so that the pid,
the signals and the exit status are the multiplexer's own."""

import os
import signal
import sys

_PACKAGE = os.path.dirname(os.path.abspath(__file__))
# Where it can be: the wheel's package data, as make's build/python also
# holds it, then //mxcontrol in Bazel's runfiles, beside the package.
_CANDIDATES = (
    os.path.join(_PACKAGE, "bin", "mxcontrol"),
    os.path.join(os.path.dirname(_PACKAGE), "mxcontrol", "mxcontrol"),
)


def binary_path() -> str:
    """The mxcontrol binary that came with this package; FileNotFoundError
    naming the places looked at when there is none."""
    for path in _CANDIDATES:
        if os.path.isfile(path):
            return path
    raise FileNotFoundError("no mxcontrol came with this package: looked at %s" % " and ".join(_CANDIDATES))


def main() -> None:
    """Runs the package's mxcontrol with this process's arguments, in its
    place. SIGPIPE and SIGXFSZ, which Python ignores and an exec would leave
    ignored, go back to their defaults first. Exits 127 when there is no
    binary and 126 when it cannot be run, as a shell does."""
    try:
        path = binary_path()
    except FileNotFoundError as error:
        print("mxcontrol: %s" % error, file=sys.stderr)
        sys.exit(127)
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    signal.signal(signal.SIGXFSZ, signal.SIG_DFL)
    try:
        os.execv(path, ["mxcontrol", *sys.argv[1:]])
    except OSError as error:
        print("mxcontrol: cannot run %s: %s" % (path, error.strerror), file=sys.stderr)
        sys.exit(126)


if __name__ == "__main__":
    main()
