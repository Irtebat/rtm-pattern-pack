#!/usr/bin/env bash
# Runs every local test for one pattern: the RTM job tests, the Flink build and
# tests (which also produce the jar), and the harness tests. Nothing in the cloud
# is touched.
#
# Usage:
#   infra/scripts/test-pattern.sh <pattern-id> [--skip-flink] [--skip-harness]
#   infra/scripts/test-pattern.sh 06-tumbling
#   infra/scripts/test-pattern.sh 06-tumbling --skip-flink
#
# The pattern id is the folder name under patterns/.
#
# Python: set RTMPACK_TEST_PYTHON to an interpreter with patterns/requirements-test.txt
# installed. The same interpreter is used for the Spark workers, which avoids a
# driver and worker version mismatch.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ARG="${1:?usage: test-pattern.sh <pattern-id> [--skip-flink] [--skip-harness]}"
shift
SKIP_FLINK=false
SKIP_HARNESS=false
for a in "$@"; do
  case "$a" in
    --skip-flink) SKIP_FLINK=true ;;
    --skip-harness) SKIP_HARNESS=true ;;
    *) echo "unknown argument: $a" >&2; exit 2 ;;
  esac
done

# The pattern id is the folder name; a trailing slash (shell completion) is accepted.
DIR="${ARG%/}"
DIR="${DIR#patterns/}"
[[ "$DIR" =~ ^[0-9]{2}-[a-z0-9-]+$ && -d "$ROOT/patterns/$DIR/rtm" ]] || {
  echo "unknown pattern: $ARG (expected a folder name under patterns/, for example 01-dedup)" >&2; exit 2; }

PY="${RTMPACK_TEST_PYTHON:-python3}"
"$PY" -c 'import pyspark, pytest' 2>/dev/null || {
  echo "$PY lacks pyspark or pytest; install patterns/requirements-test.txt and set RTMPACK_TEST_PYTHON" >&2; exit 1; }
PY="$("$PY" -c 'import sys; print(sys.executable)')"

echo "== $DIR: RTM job tests"
(cd "$ROOT/patterns/$DIR/rtm" && PYSPARK_PYTHON="$PY" PYSPARK_DRIVER_PYTHON="$PY" "$PY" -m pytest -q)

if [[ "$SKIP_FLINK" != "true" ]]; then
  echo "== $DIR: Flink build and tests"
  "$ROOT/infra/scripts/build-flink.sh" "$DIR"
fi

if [[ "$SKIP_HARNESS" != "true" ]]; then
  echo "== harness tests"
  (cd "$ROOT/harness" && "$PY" -m pytest -q)
fi
echo "== $DIR: all tests passed"
