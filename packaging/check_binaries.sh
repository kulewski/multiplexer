#!/usr/bin/env bash
# Checks that what a release publishes is a release build, stripped: an
# executable or a shared object with no symbol table and no debug
# sections, and without the debug assertions, which a build compiles in
# only without NDEBUG. With --archive, a static library without debug
# sections; its symbol table is what a program links against, so it stays.
# The release workflow, packaging/deb.sh and packaging/wheels.sh run it on
# every binary they publish.
#
#   packaging/check_binaries.sh FILE...
#   packaging/check_binaries.sh --archive FILE...
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
archive=0
if [[ "${1:-}" == "--archive" ]]; then
  archive=1
  shift
fi
[[ $# -gt 0 ]] || { echo "usage: $0 [--archive] FILE..." >&2; exit 2; }

# One debug assertion's message, read from the source, so that rewording
# it cannot leave this check looking for a text no binary holds.
assertion="$(sed -n 's/.*DbgAssertMsg((x)->is_current(), "\(.*\)").*/\1/p' "$root/lib/thread_checker.h")"
[[ -n "$assertion" ]] || { echo "cannot find the thread checker's assertion in lib/thread_checker.h" >&2; exit 2; }

status=0
for file in "$@"; do
  sections="$(readelf -S --wide "$file")"
  if grep -q ' \.debug_' <<< "$sections"; then
    echo "$file: has debug sections" >&2
    status=1
  fi
  if [[ $archive == 0 ]]; then
    if grep -q ' \.symtab ' <<< "$sections"; then
      echo "$file: has a symbol table, not stripped" >&2
      status=1
    fi
    if grep -q -a -F "$assertion" "$file"; then
      echo "$file: holds debug assertions, not a release build" >&2
      status=1
    fi
  fi
done
exit $status
