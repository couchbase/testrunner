#!/usr/bin/env bash
#
# Download a Jenkins consoleText for a *completed* build.
#
# Usage: ./fetch_console.sh <build-url> [outfile]
#   ./fetch_console.sh http://qe-jenkins1.sc.couchbase.com/job/core_dump_checker/390/ console.txt
#   ./fetch_console.sh <build-url> - | ./panics.py -      # stream, nothing on disk
#
# The whole trick is `--compressed`. These logs are ~300MB of highly repetitive
# service output, and Jenkins will gzip them: build 390 came down as 9.9MB on the
# wire in 23s, expanding to the full 301,547,401 bytes.
#
# That matters because an uncompressed request does not survive the trip. The
# server closes it partway through at an unpredictable point -- "curl: (18)
# transfer closed with outstanding read data remaining" at 23MB and again at
# 109MB on consecutive attempts -- and /consoleText sends no Content-Length and
# rejects HTTP Range, so there is nothing to resume from.
#
# Resuming via /logText/progressiveText?start=N is a dead end, for the record:
# `start` indexes Jenkins' raw stored log rather than the text it serves, and
# X-Text-Size only ever reports the constant total, never a next offset. An
# earlier version advanced `start` by the bytes received and produced 520MB of
# output for a 301MB log. Splicing chunks by content does not rescue it either:
# the log repeats near-identical lines by the million, so an overlap probe
# matches in the wrong place. Compression sidesteps all of it.

set -uo pipefail

BUILD_URL="${1:?usage: fetch_console.sh <build-url> [outfile]}"
OUT="${2:-console.txt}"
BUILD_URL="${BUILD_URL%/}"

MAX_TIME="${MAX_TIME:-900}"
ATTEMPTS="${ATTEMPTS:-3}"

if [[ "$OUT" == "-" ]]; then
  exec curl -sS --compressed --max-time "$MAX_TIME" "$BUILD_URL/consoleText"
fi

# Refuse a build that is still running: its console keeps growing, so the
# download is a moving target and the classification would be partial.
building=$(curl -sS -m 60 "$BUILD_URL/api/json?tree=building" 2>/dev/null \
           | tr -d '{}" ' | sed -n 's/.*building:\([a-z]*\).*/\1/p')
if [[ "$building" == "true" ]]; then
  echo "ERROR: $BUILD_URL is still building; wait for it to finish." >&2
  exit 1
fi

for attempt in $(seq 1 "$ATTEMPTS"); do
  echo "Downloading console log, gzipped (attempt $attempt/$ATTEMPTS)..." >&2
  curl -sS --compressed --max-time "$MAX_TIME" -o "$OUT" \
       -w "  wire %{size_download} bytes in %{time_total}s\n" \
       "$BUILD_URL/consoleText" >&2
  rc=$?
  got=$(wc -c < "$OUT" 2>/dev/null | tr -d ' ')
  got="${got:-0}"

  # A complete console log for a finished build ends with Jenkins' own
  # "Finished: SUCCESS/FAILURE/ABORTED" line. Cheap and unambiguous.
  if tail -c 4096 "$OUT" 2>/dev/null | tr -d '\r' | grep -q '^Finished: '; then
    echo "Done: $OUT ($got bytes, $(tail -c 4096 "$OUT" | tr -d '\r' \
          | grep '^Finished: ' | tail -1))" >&2
    exit 0
  fi

  echo "  incomplete after $got bytes (curl rc=$rc); no 'Finished:' line." >&2
  [[ "$attempt" -lt "$ATTEMPTS" ]] && sleep 10
done

echo "ERROR: could not download a complete console log after $ATTEMPTS attempts." >&2
exit 1
