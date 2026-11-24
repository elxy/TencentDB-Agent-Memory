#!/usr/bin/env bash
# Replay the ernest trim onto a MemoryCore checkout.
#
# Usage: replay.sh <trimmed-snapshot-dir> [repo-root]
#   <trimmed-snapshot-dir>  the trimmed MemoryCore source tree (the snapshot that
#                           has src/ at its root; no node_modules needed).
#   [repo-root]             root of this repository (contains MemoryCore/); default: repo of this script.
#
# The manifest is generated from a blob-level comparison against the base tag, so
# replaying it onto the same base commit is deterministic: the script refuses when
# the base commit does not match.
set -euo pipefail

SNAPSHOT=${1:?usage: replay.sh <trimmed-snapshot-dir> [repo-root]}
REPO=${2:-$(cd "$(dirname "$0")/.." && pwd)}
MANIFEST="$(cd "$(dirname "$0")" && pwd)/manifest.json"
TARGET="$REPO/MemoryCore"

[ -d "$SNAPSHOT/src" ] || { echo "snapshot has no src/: $SNAPSHOT" >&2; exit 2; }
[ -d "$TARGET" ] || { echo "no MemoryCore/ under: $REPO" >&2; exit 2; }

base=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["base_commit"])' "$MANIFEST")
head=$(git -C "$REPO" rev-parse HEAD)
if [ "$head" != "$base" ]; then
  echo "refusing to replay: HEAD is $head, manifest base is $base" >&2
  echo "checkout the base first: git -C $REPO checkout -b ernest $base" >&2
  exit 3
fi

python3 - "$MANIFEST" <<'PY' > /tmp/ernest-trim/.plan
import json, sys
m = json.load(open(sys.argv[1]))
for item in m["modified"]:
    print("M\t" + item["path"])
for item in m["added"]:
    print("A\t" + item["path"])
for path in m["removed"]:
    print("D\t" + path)
PY

while IFS=$'\t' read -r kind path; do
  case "$kind" in
    M|A)
      [ -f "$SNAPSHOT/$path" ] || { echo "missing in snapshot: $path" >&2; exit 4; }
      mkdir -p "$TARGET/$(dirname "$path")"
      cp -p "$SNAPSHOT/$path" "$TARGET/$path"
      ;;
    D)
      git -C "$REPO" rm -q -- "MemoryCore/$path"
      ;;
    *) echo "bad plan line: $kind $path" >&2; exit 5 ;;
  esac
done < /tmp/ernest-trim/.plan

echo "replayed $(grep -c '^[MA]' /tmp/ernest-trim/.plan) writes and $(grep -c '^D' /tmp/ernest-trim/.plan) removals into $TARGET"
