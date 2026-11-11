#!/usr/bin/env bash
# Builds a pattern's Flink application jar (and runs its unit tests).
# Usage: infra/scripts/build-flink.sh <pattern-id> [extra Maven arguments]   e.g. 01-dedup
#
# Maven downloads come from Maven Central (or from your own ~/.m2/settings.xml).
# Optional:
#   RTMPACK_MAVEN_MIRROR    URL of a Maven repository mirror for all downloads
#                           (applied through infra/scripts/maven-settings.xml)
#   RTMPACK_MAVEN_SETTINGS  path of a Maven settings file to use instead
set -euo pipefail

PATTERN="${1:?usage: build-flink.sh <pattern-id>}"
PATTERN="${PATTERN%/}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROJECT="$ROOT/patterns/$PATTERN/flink"
[[ -f "$PROJECT/pom.xml" ]] || { echo "no Flink project at $PROJECT" >&2; exit 2; }

if [[ -z "${JAVA_HOME:-}" && -d "$HOME/.sdkman/candidates/java/current" ]]; then
  export JAVA_HOME="$HOME/.sdkman/candidates/java/current"
fi

SETTINGS=()
if [[ -n "${RTMPACK_MAVEN_SETTINGS:-}" ]]; then
  SETTINGS=(-s "$RTMPACK_MAVEN_SETTINGS")
elif [[ -n "${RTMPACK_MAVEN_MIRROR:-}" ]]; then
  export RTMPACK_MAVEN_MIRROR
  SETTINGS=(-s "$ROOT/infra/scripts/maven-settings.xml")
fi

command mvn -B -q ${SETTINGS[@]+"${SETTINGS[@]}"} -f "$PROJECT/pom.xml" clean package "${@:2}"
ls -l "$PROJECT"/target/*.jar
