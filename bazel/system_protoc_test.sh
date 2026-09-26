#!/usr/bin/env bash
# bazel/system_protoc.sh in front of a protoc without --pyi_out, as 3.12 is:
# the flag is left out of the call and every other argument passed on as
# given, and each .proto gets its stub where the include paths put protoc's
# outputs; a protoc that fails leaves no stub. In front of a protoc with the
# flag, the call is passed on whole and the wrapper writes nothing.
set -euo pipefail
wrapper="$PWD/bazel/system_protoc.sh"
work="${TEST_TMPDIR:-$(mktemp -d)}"
failures=0

# A protoc as the wrapper sees it. `old` has no --pyi_out in its help and
# fails on the flag, as 3.12 does with no protoc-gen-pyi plugin; `new` has
# it. Both write their arguments, one per line, to $CALLS, and fail on a
# file named bad.proto.
fake_protoc() {
  local kind=$1
  mkdir -p "$work/$kind"
  cat > "$work/$kind/protoc" << EOF
#!/usr/bin/env bash
if [[ "\$1" == --help ]]; then
  echo "  --python_out=OUT_DIR        Generate Python source file."
  [[ $kind == new ]] && echo "  --pyi_out=OUT_DIR           Generate python pyi stub."
  exit 0
fi
printf '%s\n' "\$@" > "\$CALLS"
for argument in "\$@"; do
  if [[ $kind == old && "\$argument" == --pyi_out* ]]; then
    echo "protoc-gen-pyi: program not found or is not executable" >&2
    exit 1
  fi
  [[ "\$argument" == */bad.proto ]] && exit 1
done
exit 0
EOF
  chmod +x "$work/$kind/protoc"
}
fake_protoc old
fake_protoc new

# Fails the test, naming what was expected.
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# Runs the wrapper in front of the `kind` protoc with the remaining arguments.
run() {
  local kind=$1
  shift
  CALLS="$work/calls" PATH="$work/$kind:$PATH" "$wrapper" "$@"
}

# The old protoc, an include path as the genrules give it for @mx, two files.
out="$work/out1"
run old -I external/mx --cpp_out="$out" --python_out="$out" --pyi_out="$out" \
  external/mx/multiplexer/Multiplexer.proto external/mx/lib/logging/Logging.proto \
  || fail "the wrapper failed in front of a protoc without --pyi_out"
expected="-I
external/mx
--cpp_out=$out
--python_out=$out
external/mx/multiplexer/Multiplexer.proto
external/mx/lib/logging/Logging.proto"
[[ "$(cat "$work/calls" 2> /dev/null)" == "$expected" ]] \
  || fail "the call without --pyi_out and otherwise as given; it was: $(cat "$work/calls" 2> /dev/null)"
for stub in multiplexer/Multiplexer_pb2.pyi lib/logging/Logging_pb2.pyi; do
  grep -q '^def __getattr__(name: str) -> Any: \.\.\.$' "$out/$stub" 2> /dev/null || fail "a stub making every name Any at $stub"
done

# The include path as the genrules give it in this workspace, ".", and a
# file given as ./path, with the space-separated forms of both flags.
out="$work/out2"
run old -I . --python_out="$out" --pyi_out "$out" ./multiplexer/Recording.proto || fail "the wrapper failed with -I ."
grep -q '__getattr__' "$out/multiplexer/Recording_pb2.pyi" 2> /dev/null || fail "a stub at multiplexer/Recording_pb2.pyi"

# A protoc that fails: the failure is the wrapper's, and nothing is written.
out="$work/out3"
if run old -I . --python_out="$out" --pyi_out="$out" multiplexer/bad.proto 2> /dev/null; then
  fail "the wrapper succeeded where protoc failed"
fi
[[ ! -e "$out/multiplexer/bad_pb2.pyi" ]] || fail "no stub where protoc failed"

# The new protoc: the call passed on whole, --pyi_out included, and no stub
# of the wrapper's.
out="$work/out4"
run new -I . --python_out="$out" --pyi_out="$out" multiplexer/events.proto || fail "the wrapper failed in front of protoc 3.20"
expected="-I
.
--python_out=$out
--pyi_out=$out
multiplexer/events.proto"
[[ "$(cat "$work/calls" 2> /dev/null)" == "$expected" ]] || fail "the call passed on whole; it was: $(cat "$work/calls" 2> /dev/null)"
[[ ! -e "$out/multiplexer/events_pb2.pyi" ]] || fail "no stub of the wrapper's in front of protoc 3.20"

if ((failures)); then
  echo "$failures check(s) failed" >&2
  exit 1
fi
echo "system_protoc.sh: every check passed"
