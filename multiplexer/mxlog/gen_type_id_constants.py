"""Turn a file of `NAME = int` lines into a C++ header of constants.

Usage: gen_type_id_constants.py type_id_constants.txt type_id_constants.h
The namespace is the output file's basename without extension.
"""

import os
import re
import sys


def main(src: str, out: str) -> None:
    """Read `src`, write the header `out`."""
    consts = []
    with open(src) as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)[lL]?$", line)
            if not m:
                sys.exit("%s: cannot parse %r" % (src, raw.rstrip()))
            consts.append("  static const unsigned int %s = %s;" % m.groups())
    namespace = os.path.splitext(os.path.basename(out))[0]
    # Guard derived from the workspace-relative path, like every other header.
    rel = out.split("/bin/", 1)[1] if "/bin/" in out else out
    guard = "MX_" + re.sub(r"[^A-Za-z0-9]", "_", rel).upper() + "_"
    with open(out, "w") as f:
        f.write("// Generated from %s; do not edit.\n" % os.path.basename(src))
        f.write(
            "#ifndef %s\n#define %s\n\nnamespace %s {\n%s\n} // namespace %s\n\n#endif // %s\n"
            % (guard, guard, namespace, "\n".join(consts), namespace, guard)
        )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
