#!/usr/bin/env bash
# The default for //:protoc: whatever protoc is on PATH.
#
# protoc writes the stubs of the Python modules it generates (--pyi_out)
# from 3.20 on; an older one, Ubuntu 22.04's 3.12, takes the flag for a
# plugin's and fails. With such a protoc the flag is left out, and each
# .proto given gets, where protoc would have written its stub, one that
# makes every name of the module Any: a type checker then accepts the code
# that uses the module, as it would a module without a stub.
set -euo pipefail
help="$(protoc --help 2>&1 || true)"
if [[ "$help" == *--pyi_out* ]]; then
  exec protoc "$@"
fi

arguments=()
includes=()
protos=()
pyi_out=""
while (($#)); do
  case "$1" in
    --pyi_out=*) pyi_out="${1#--pyi_out=}" ;;
    --pyi_out)
      pyi_out="$2"
      shift
      ;;
    -I | --proto_path)
      includes+=("$2")
      arguments+=("$1" "$2")
      shift
      ;;
    -I*)
      includes+=("${1#-I}")
      arguments+=("$1")
      ;;
    --proto_path=*)
      includes+=("${1#--proto_path=}")
      arguments+=("$1")
      ;;
    *.proto)
      protos+=("$1")
      arguments+=("$1")
      ;;
    *) arguments+=("$1") ;;
  esac
  shift
done
protoc "${arguments[@]}"
[[ -n "$pyi_out" ]] || exit 0

((${#includes[@]})) || includes=(.)
for proto in "${protos[@]}"; do
  # The name protoc gives the file: its path within the first include path
  # that holds it, which is where protoc writes what it generates for it.
  relative="${proto#./}"
  for include in "${includes[@]}"; do
    include="${include%/}"
    if [[ "$include" == "." || -z "$include" ]]; then
      break
    elif [[ "$proto" == "$include"/* ]]; then
      relative="${proto#"$include"/}"
      break
    fi
  done
  stub="$pyi_out/${relative%.proto}_pb2.pyi"
  mkdir -p "$(dirname "$stub")"
  cat > "$stub" << 'EOF'
# Written by bazel/system_protoc.sh: the protoc that generated this module
# is older than 3.20 and writes no stubs, so every name here is Any.
from typing import Any

def __getattr__(name: str) -> Any: ...
EOF
done
