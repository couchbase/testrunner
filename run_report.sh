#!/usr/bin/env bash
#
# Jenkins entry point for the panic_classifier job: fetch an upstream
# core_dump_checker console log, classify every panic in it by component, and
# drop the reports in ./reports for archiving.
#
# Inputs are read from the environment so they can be Jenkins parameters:
#
#   BUILD_URL_TO_SCAN  full upstream build URL; overrides UPSTREAM_* below
#   UPSTREAM_JOB       upstream job name             (default: core_dump_checker)
#   UPSTREAM_BUILD     upstream build number         (default: lastCompletedBuild)
#   JENKINS_BASE       Jenkins root URL              (default: $JENKINS_URL)
#   ALERT_COMPONENTS   comma list of components that should make the build
#                      UNSTABLE when they have panics, e.g. "search,index".
#                      Empty (default) = report only, always SUCCESS.
#   ALERT_MIN_SITES    ignore alert components with fewer than this many crash
#                      sites                          (default: 1)
#   KEEP_CONSOLE       1 keeps the ~300MB log in the workspace (default: 0)
#
# Exit codes: 0 = ok, 1 = fetch/parse failure, 2 = ok but ALERT_COMPONENTS hit.

set -uo pipefail

cd "$(dirname "$0")"

UPSTREAM_JOB="${UPSTREAM_JOB:-core_dump_checker}"
UPSTREAM_BUILD="${UPSTREAM_BUILD:-lastCompletedBuild}"
JENKINS_BASE="${JENKINS_BASE:-${JENKINS_URL:-http://qe-jenkins1.sc.couchbase.com/}}"
ALERT_COMPONENTS="${ALERT_COMPONENTS:-}"
ALERT_MIN_SITES="${ALERT_MIN_SITES:-1}"
KEEP_CONSOLE="${KEEP_CONSOLE:-0}"

if [[ -n "${BUILD_URL_TO_SCAN:-}" ]]; then
  TARGET="${BUILD_URL_TO_SCAN%/}"
else
  TARGET="${JENKINS_BASE%/}/job/${UPSTREAM_JOB}/${UPSTREAM_BUILD}"
fi

CONSOLE="console.txt"
OUT="reports"
rm -rf "$OUT"
mkdir -p "$OUT"

echo "=========================================================================="
echo "Scanning : $TARGET"
echo "Workspace: $PWD"
echo "=========================================================================="

# Resolve "lastCompletedBuild" to a real number so the report names the build.
RESOLVED=$(curl -sS -m 60 "$TARGET/api/json?tree=number" 2>/dev/null \
           | tr ',' '\n' | sed -n 's/.*"number":\([0-9]*\).*/\1/p' | head -1)
if [[ -n "${RESOLVED:-}" ]]; then
  echo "Upstream build number: $RESOLVED"
  echo "$RESOLVED" > "$OUT/upstream_build.txt"
fi
echo "$TARGET" > "$OUT/upstream_url.txt"

if ! ./fetch_console.sh "$TARGET" "$CONSOLE"; then
  echo "ERROR: could not download the console log from $TARGET" >&2
  exit 1
fi

echo
echo "--- classifying ---"
# One parse writes every variant, including reports/by-component/<comp>.txt.
# Re-running the script per report would re-read the ~300MB log each time.
if ! ./panics.py "$CONSOLE" --all-reports "$OUT"; then
  echo "ERROR: panics.py failed" >&2
  exit 1
fi

echo
echo "--- panics by component ---"
for f in "$OUT"/by-component/*.txt; do
  [[ -e "$f" ]] || continue
  comp=$(basename "$f" .txt)
  printf '%-14s ' "$comp"
  grep -m1 '^Showing' "$f" | sed 's/Showing *: //; s/ *(.*//'
done

echo
echo "--- top signatures ---"
grep -E "^(#[0-9]+ |  panic :|  frame :|  how   :)" "$OUT/panics_all.txt" \
  | head -60 | cut -c1-140 || true

if [[ "$KEEP_CONSOLE" != "1" ]]; then
  echo
  echo "Removing $CONSOLE (set KEEP_CONSOLE=1 to keep it)"
  rm -f "$CONSOLE"
fi

echo
echo "Reports written to $PWD/$OUT"

# Gate on the components this job was told to care about. Evaluated from
# metrics.json so nothing has to scrape the text reports.
if [[ -z "$ALERT_COMPONENTS" ]]; then
  echo "ALERT_COMPONENTS not set -- reporting only."
  exit 0
fi

python3 - "$OUT/metrics.json" "$ALERT_COMPONENTS" "$ALERT_MIN_SITES" <<'PY'
import json, sys
metrics_path, wanted_raw, min_sites = sys.argv[1], sys.argv[2], int(sys.argv[3])
with open(metrics_path) as fh:
    m = json.load(fh)
wanted = [c.strip() for c in wanted_raw.split(",") if c.strip()]
by_comp = m.get("by_component", {})
hits = []
for comp in wanted:
    entry = by_comp.get(comp)
    if entry and entry["crash_sites"] >= min_sites:
        hits.append((comp, entry["signatures"], entry["crash_sites"]))
if not hits:
    print("No panics in alert components (%s)." % ", ".join(wanted))
    sys.exit(0)
for comp, sigs, sites in hits:
    print("ALERT %s: %d signature(s), %d crash site(s)" % (comp, sigs, sites))
sys.exit(2)
PY
exit $?
