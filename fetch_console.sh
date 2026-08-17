#!/usr/bin/env bash
# Robustly download a huge Jenkins consoleText.
#
# Jenkins streams /consoleText with no Content-Length and no byte-range support,
# so a single curl gets cut off partway through a ~300MB log ("transfer closed
# with outstanding read data remaining"). The progressiveText API *does* accept a
# start offset, so we fetch in chunks, advancing by however many bytes actually
# landed. A truncated chunk is harmless: we just resume from where it stopped.
#
# Usage: ./fetch_console.sh <build-url> [outfile]
#   ./fetch_console.sh http://qe-jenkins1.sc.couchbase.com/job/core_dump_checker/390/ console.txt
#
# Re-running with an existing outfile resumes where it left off. Pass "-" as the
# outfile to stream to stdout instead (no resume), e.g.
#   ./fetch_console.sh <build-url> - | ./panics.py -

set -uo pipefail

BUILD_URL="${1:?usage: fetch_console.sh <build-url> [outfile]}"
OUT="${2:-console.txt}"
BUILD_URL="${BUILD_URL%/}"
PROG="$BUILD_URL/logText/progressiveText"

# Seconds per chunk attempt. Each attempt keeps whatever it received.
CHUNK_SECONDS="${CHUNK_SECONDS:-100}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-200}"

total=$(curl -sS -m 60 -D - -o /dev/null "$PROG?start=0" 2>/dev/null \
        | tr -d '\r' | awk 'tolower($1)=="x-text-size:"{print $2}')

if [[ -z "${total:-}" ]]; then
  echo "Could not read X-Text-Size from $PROG -- is the build URL right / VPN up?" >&2
  exit 1
fi
echo "Total log size: $total bytes ($((total / 1048576)) MB)" >&2

# Resume support: if OUT already exists, pick up at its current size.
offset=0
if [[ "$OUT" == "-" ]]; then
  :   # streaming to stdout; nothing to resume from
elif [[ -f "$OUT" ]]; then
  offset=$(wc -c < "$OUT" | tr -d ' ')
  if (( offset >= total )); then
    echo "$OUT already has $offset bytes; nothing to do." >&2
    exit 0
  fi
  echo "Resuming existing $OUT at offset $offset" >&2
else
  : > "$OUT"
fi

tmp=$(mktemp "${TMPDIR:-/tmp}/jenkins-chunk.XXXXXX")
trap 'rm -f "$tmp"' EXIT

attempt=0
while (( offset < total )); do
  (( attempt++ ))
  if (( attempt > MAX_ATTEMPTS )); then
    echo "Giving up after $MAX_ATTEMPTS attempts at offset $offset/$total" >&2
    exit 1
  fi

  # Partial transfers are expected; ignore curl's exit status and use byte count.
  curl -sS -m "$CHUNK_SECONDS" -o "$tmp" "$PROG?start=$offset" 2>/dev/null
  got=$(wc -c < "$tmp" | tr -d ' ')

  if (( got == 0 )); then
    echo "  chunk at $offset returned 0 bytes; retrying in 5s" >&2
    sleep 5
    continue
  fi

  # Chunks are contiguous byte ranges, so plain concatenation rebuilds the file
  # exactly -- including lines split across a chunk boundary.
  if [[ "$OUT" == "-" ]]; then
    cat "$tmp"
  else
    cat "$tmp" >> "$OUT"
  fi
  offset=$(( offset + got ))
  printf '  %d / %d bytes (%d%%)\n' "$offset" "$total" "$(( offset * 100 / total ))" >&2
done

echo "Done: $OUT ($offset bytes)" >&2
