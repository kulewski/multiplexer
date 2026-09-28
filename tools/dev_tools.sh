# The Python development tools, pinned in requirements-dev.txt, in a
# virtual environment of their own, .tools/venv: made on first use and
# brought in line with the file whenever it changes. Sourced by format.sh
# and check.sh from the repository's root; DEV_TOOLS is the environment's
# bin/, where black, ruff, pyright and a python with yaml are.
DEV_TOOLS=.tools/venv/bin
if ! cmp -s requirements-dev.txt .tools/venv/requirements-dev.txt; then
  [[ -x $DEV_TOOLS/pip ]] || python3 -m venv .tools/venv
  "$DEV_TOOLS/pip" install --quiet --disable-pip-version-check -r requirements-dev.txt >&2
  cp requirements-dev.txt .tools/venv/requirements-dev.txt
fi
