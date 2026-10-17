#!/usr/bin/env bash
# mxcontrol under valgrind's memcheck, for the scenarios ./check.sh
# --valgrind runs, whose harness starts every multiplexer as $MXCONTROL:
# the binary MX_MEMCHECK_MXCONTROL names, each value never initialized that
# it branches on or hands the kernel reported with where it came from, into
# the test's outputs (memcheck-mxcontrol.PID.log), and the exit 99 on such
# an error, which fails the scenario at the Cluster's check of the exit
# codes. Leaks are LeakSanitizer's (./check.sh --leaks); RE2's reads of what
# it never wrote, on purpose, are suppressed (tools/memcheck.supp).
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec valgrind --tool=memcheck --error-exitcode=99 --track-origins=yes --leak-check=no \
  --suppressions="$here/memcheck.supp" --log-file="${TEST_UNDECLARED_OUTPUTS_DIR:-/tmp}/memcheck-mxcontrol.%p.log" \
  "${MX_MEMCHECK_MXCONTROL:?the mxcontrol binary to run under memcheck}" "$@"
