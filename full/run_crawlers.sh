#!/usr/bin/env bash
# Reproducible launcher for all legal-text crawlers.
#
# Usage:
#   ./full/run_crawlers.sh                 # run everything (full download; hours!)
#   ./full/run_crawlers.sh --dry-run       # enumerate sources + write manifests only
#   ./full/run_crawlers.sh --limit 10      # test mode: max 10 docs per source/state
#   ./full/run_crawlers.sh --refresh-index # refresh cached source inventories first
#   ./full/run_crawlers.sh --defer-on-waf eu
#                                           # preserve audited EU WAF deferrals
#   ./full/run_crawlers.sh --refresh-existing laender_verfassungen
#                                           # re-fetch static constitution URLs
#   ./full/run_crawlers.sh eu bund         # run only the named crawlers
#
# Crawler names: eu, bund, laender_verfassungen, laender_juris, laender_by_bb_sn,
#                laender_hb_ni_nw, laender_assets
#
# All crawlers are idempotent and resumable: re-running skips existing files.
# Data and logs default to the project Dropbox locations. Override them with
# EU_REGULATION_DATA_DIR, EU_REGULATION_SHARED_DIR, or EU_REGULATION_LOG_DIR.

set -uo pipefail
cd "$(dirname "$0")/.."   # repo root

PYTHON="${PYTHON:-python3}"
EU_REGULATION_DATA_DIR="${EU_REGULATION_DATA_DIR:-$HOME/Dropbox/Projects/EU_Regulation/Data}"
EU_REGULATION_SHARED_DIR="${EU_REGULATION_SHARED_DIR:-$HOME/Dropbox/Papers/00_Ideas/EU_Regulation}"
EU_REGULATION_LOG_DIR="${EU_REGULATION_LOG_DIR:-$EU_REGULATION_SHARED_DIR/logs/crawlers}"
export EU_REGULATION_DATA_DIR EU_REGULATION_SHARED_DIR EU_REGULATION_LOG_DIR
LOGDIR="$EU_REGULATION_LOG_DIR"
mkdir -p "$EU_REGULATION_DATA_DIR" "$LOGDIR"

echo "[paths] data: $EU_REGULATION_DATA_DIR"
echo "[paths] logs: $LOGDIR"

EXTRA_ARGS=()
SELECTED=()
REFRESH_INDEX=0
REFRESH_EXISTING=0
DEFER_ON_WAF=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)  EXTRA_ARGS+=("--dry-run"); shift ;;
    --refresh-index) REFRESH_INDEX=1; shift ;;
    --refresh-existing) REFRESH_EXISTING=1; shift ;;
    --defer-on-waf) DEFER_ON_WAF=1; shift ;;
    --limit)
      if [[ $# -lt 2 || ! "$2" =~ ^[0-9]+$ ]]; then
        echo "[error] --limit requires a non-negative integer" >&2
        exit 2
      fi
      EXTRA_ARGS+=("--limit" "$2"); shift 2 ;;
    --*)
      echo "[error] unknown option: $1" >&2
      exit 2 ;;
    *) SELECTED+=("$1"); shift ;;
  esac
done

ALL=(eu bund laender_verfassungen laender_juris laender_by_bb_sn laender_hb_ni_nw laender_assets)
if [[ ${#SELECTED[@]} -eq 0 ]]; then
  SELECTED=("${ALL[@]}")
fi

# Repeated logical selectors must not start duplicate writers for the same
# state folder. Preserve first-appearance order while dropping duplicates.
DEDUPED=()
for candidate in "${SELECTED[@]}"; do
  seen=0
  for existing in ${DEDUPED[@]+"${DEDUPED[@]}"}; do
    if [[ "$candidate" == "$existing" ]]; then
      seen=1
      break
    fi
  done
  if [[ $seen -eq 0 ]]; then
    DEDUPED+=("$candidate")
  else
    echo "[warning] duplicate crawler selection ignored: $candidate" >&2
  fi
done
SELECTED=("${DEDUPED[@]}")

# Reject an invalid invocation before starting any child process. This prevents a typo
# among otherwise valid selectors from launching a partial, unintended crawl.
for candidate in "${SELECTED[@]}"; do
  known=0
  for available in "${ALL[@]}"; do
    if [[ "$candidate" == "$available" ]]; then
      known=1
      break
    fi
  done
  if [[ $known -eq 0 ]]; then
    echo "[error] unknown crawler: $candidate (known: ${ALL[*]})" >&2
    exit 2
  fi
done

declare -a PIDS=()
declare -a NAMES=()
FAIL=0
ROOT_BY_SCHEDULED=0
ROOT_BY_OK=0
ROOT_SN_SCHEDULED=0
ROOT_SN_OK=0
ROOT_NW_SCHEDULED=0
ROOT_NW_OK=0

stop_children() {
  local exit_code="$1"
  if [[ ${#PIDS[@]} -gt 0 ]]; then
    kill "${PIDS[@]}" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
  fi
  exit "$exit_code"
}
trap 'stop_children 130' INT
trap 'stop_children 143' TERM

run_crawler() {
  local name="$1"; shift
  local script="full/crawlers/$1"; shift
  if [[ ! -f "$script" ]]; then
    echo "[FAILED] $name: $script not found" >&2
    FAIL=1
    return
  fi
  echo "[start] $name -> $LOGDIR/$name.log"
  "$PYTHON" "$script" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} "$@" >"$LOGDIR/$name.log" 2>&1 &
  PIDS+=("$!")
  NAMES+=("$name")
}

run_indexed_crawler() {
  local name="$1"
  local script="$2"
  shift 2
  if [[ $REFRESH_INDEX -eq 1 ]]; then
    run_crawler "$name" "$script" --refresh-index "$@"
  else
    run_crawler "$name" "$script" "$@"
  fi
}

wait_for_crawlers() {
  local i
  for i in "${!PIDS[@]}"; do
    if wait "${PIDS[$i]}"; then
      echo "[done] ${NAMES[$i]}"
      case "${NAMES[$i]}" in
        laender_by_bb_sn_by) ROOT_BY_OK=1 ;;
        laender_by_bb_sn_sn) ROOT_SN_OK=1 ;;
        laender_hb_ni_nw_nw) ROOT_NW_OK=1 ;;
      esac
    else
      echo "[FAILED] ${NAMES[$i]} (see $LOGDIR/${NAMES[$i]}.log)"
      FAIL=1
    fi
  done
  PIDS=()
  NAMES=()
}

RUN_ASSETS=0
for name in "${SELECTED[@]}"; do
  case "$name" in
    eu)
      if [[ $DEFER_ON_WAF -eq 1 ]]; then
        run_indexed_crawler eu crawl_eu.py --defer-on-waf
      else
        run_indexed_crawler eu crawl_eu.py
      fi ;;
    bund)                  run_crawler bund crawl_bund.py ;;
    laender_verfassungen)
      if [[ $REFRESH_EXISTING -eq 1 ]]; then
        run_crawler laender_verfassungen crawl_laender_verfassungen.py --refresh-existing
      else
        run_crawler laender_verfassungen crawl_laender_verfassungen.py
      fi ;;
    laender_juris)
      for state in bw be hh he mv rp sl st sh th; do
        run_indexed_crawler "laender_juris_$state" crawl_laender_juris.py --state "$state"
      done ;;
    laender_by_bb_sn)
      ROOT_BY_SCHEDULED=1
      ROOT_SN_SCHEDULED=1
      for state in by bb sn; do
        run_indexed_crawler "laender_by_bb_sn_$state" crawl_laender_by_bb_sn.py --state "$state"
      done ;;
    laender_hb_ni_nw)
      ROOT_NW_SCHEDULED=1
      for state in hb ni nw; do
        run_indexed_crawler "laender_hb_ni_nw_$state" crawl_laender_hb_ni_nw.py --state "$state"
      done ;;
    laender_assets)        RUN_ASSETS=1 ;;
    *)
      echo "[error] unknown crawler: $name (known: ${ALL[*]})" >&2
      FAIL=1 ;;
  esac
done

wait_for_crawlers

# Asset discovery depends on completed local BY/SN/NW root HTML, so it is a
# second phase when selected together with the root crawlers.
if [[ $RUN_ASSETS -eq 1 ]]; then
  if [[ $ROOT_BY_SCHEDULED -eq 0 || $ROOT_BY_OK -eq 1 ]]; then
    run_indexed_crawler laender_assets_by crawl_laender_assets.py --state by
  else
    echo "[SKIPPED] laender_assets_by: its root crawl failed" >&2
  fi
  if [[ $ROOT_SN_SCHEDULED -eq 0 || $ROOT_SN_OK -eq 1 ]]; then
    run_indexed_crawler laender_assets_sn crawl_laender_assets.py --state sn
  else
    echo "[SKIPPED] laender_assets_sn: its root crawl failed" >&2
  fi
  if [[ $ROOT_NW_SCHEDULED -eq 0 || $ROOT_NW_OK -eq 1 ]]; then
    run_indexed_crawler laender_assets_nw crawl_laender_assets.py --state nw
  else
    echo "[SKIPPED] laender_assets_nw: its root crawl failed" >&2
  fi
  wait_for_crawlers
fi
trap - INT TERM
exit $FAIL
