"""Writes the .pyi stub of multiplexer._native from the built extension,
with pybind11-stubgen, which reads the signatures pybind11 puts in the
docstrings. Usage: native_stub.py _native.so out.pyi"""

import os
import shutil
import sys
import tempfile

import pybind11_stubgen


def main(extension: str, out: str) -> int:
    """Import the extension as multiplexer._native from a scratch tree, run
    the generator in this process, and move the stub it writes to `out`."""
    with tempfile.TemporaryDirectory() as scratch:
        package = os.path.join(scratch, "multiplexer")
        os.makedirs(package)
        shutil.copy(extension, os.path.join(package, "_native.so"))
        sys.path.insert(0, scratch)
        pybind11_stubgen.main(["multiplexer._native", "-o", os.path.join(scratch, "out")])
        shutil.copy(os.path.join(scratch, "out", "multiplexer", "_native.pyi"), out)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
