# The development tools, pinned: the ones in requirements-dev.txt, in a
# virtual environment of their own, .tools/venv, made on first use and
# brought in line with the file whenever it changes, and buildifier, the
# release binary of the version below, checked against its digest, in the
# same bin/. Sourced by format.sh and check.sh from the repository's root;
# DEV_TOOLS is that bin/, where black, ruff, pyright, clang-format,
# buildifier and a python with yaml are. CI formats with the same tools.
DEV_TOOLS=.tools/venv/bin
if ! cmp -s requirements-dev.txt .tools/venv/requirements-dev.txt; then
  [[ -x $DEV_TOOLS/pip ]] || python3 -m venv .tools/venv
  "$DEV_TOOLS/pip" install --quiet --disable-pip-version-check -r requirements-dev.txt >&2
  cp requirements-dev.txt .tools/venv/requirements-dev.txt
fi

# PyPI has no buildifier: the one for this machine is fetched when the
# binary in DEV_TOOLS is missing or another.
BUILDIFIER_VERSION=v10.1.0
case "$(uname -sm)" in
  "Linux x86_64") buildifier=(buildifier-linux-amd64 31b6a8aa1e5c746696788f428729701770ad91925873d8256cb885c60e12c77e) ;;
  "Linux aarch64") buildifier=(buildifier-linux-arm64 38d2ed845f560b4a16ddee41de906508a95f8dc85b04e0851b0a71e3a70d3890) ;;
  *) echo "tools/dev_tools.sh: no buildifier pinned for $(uname -sm)" >&2 && exit 1 ;;
esac
if ! sha256sum --check --status <<<"${buildifier[1]}  $DEV_TOOLS/buildifier" 2>/dev/null; then
  curl -fsSL -o "$DEV_TOOLS/buildifier.part" \
    "https://github.com/bazelbuild/buildtools/releases/download/$BUILDIFIER_VERSION/${buildifier[0]}"
  sha256sum --check --status <<<"${buildifier[1]}  $DEV_TOOLS/buildifier.part" ||
    { echo "tools/dev_tools.sh: ${buildifier[0]} $BUILDIFIER_VERSION does not match its digest" >&2 && exit 1; }
  chmod +x "$DEV_TOOLS/buildifier.part"
  mv "$DEV_TOOLS/buildifier.part" "$DEV_TOOLS/buildifier"
fi
