#!/usr/bin/env bash
# ============================================================
# Lakebridge end-to-end demo driver — Snowflake (Morpheus) path
#
# Usage:
#   ./run_demo.sh                   # full flow: install + analyze + transpile
#   ./run_demo.sh analyze           # analyzer only
#   ./run_demo.sh transpile         # transpile only (assumes install-transpile done)
#   PROFILE=my-profile ./run_demo.sh
#
# Reads from:  ./snowflake, ./redshift
# Writes to:   ./analyzer_output, ./transpiled/*, ./logs/*
# ============================================================
set -euo pipefail

PROFILE="${PROFILE:-lakebridge-demo}"
ROOT="$(cd "$(dirname "$0")" && pwd)"
CATALOG="${CATALOG:-lakebridge_demo}"
SCHEMA="${SCHEMA:-migration_test}"

# ---- THE Databricks-laptop gotcha ----------------------------
# Without this, install-transpile cannot reach Maven Central
# from a Databricks-issued laptop and silently hangs (or 4xx's
# on SSL). Override only if you have a different mirror.
: "${LAKEBRIDGE_MAVEN_URL:=https://maven-proxy.cloud.databricks.com}"
export LAKEBRIDGE_MAVEN_URL
echo "[lakebridge] LAKEBRIDGE_MAVEN_URL=$LAKEBRIDGE_MAVEN_URL"
# --------------------------------------------------------------

step() { printf "\n\033[1;36m==> %s\033[0m\n" "$*"; }

cmd_install() {
  step "Installing Lakebridge CLI"
  databricks labs install lakebridge --profile "$PROFILE"

  step "Installing Morpheus transpiler (interactive)"
  echo "When prompted, use these absolute paths:"
  echo "  Input:  $ROOT/snowflake"
  echo "  Output: $ROOT/transpiled/snowflake"
  echo "  Error:  $ROOT/logs/snowflake_errors.log"
  echo "  Catalog: $CATALOG   Schema: $SCHEMA"
  databricks labs lakebridge install-transpile --profile "$PROFILE"
}

cmd_analyze() {
  step "Analyzing Snowflake corpus"
  databricks labs lakebridge analyze \
    --source-directory "$ROOT/snowflake" \
    --report-file      "$ROOT/analyzer_output/snowflake_inventory.xlsx" \
    --source-tech      Snowflake \
    --generate-json    true \
    --profile "$PROFILE"

  step "Analyzing Redshift corpus"
  databricks labs lakebridge analyze \
    --source-directory "$ROOT/redshift" \
    --report-file      "$ROOT/analyzer_output/redshift_inventory.xlsx" \
    --source-tech      Redshift \
    --generate-json    true \
    --profile "$PROFILE"
}

cmd_transpile() {
  step "Transpiling Snowflake → Databricks (Morpheus)"
  databricks labs lakebridge transpile \
    --source-dialect  snowflake \
    --input-source    "$ROOT/snowflake" \
    --output-folder   "$ROOT/transpiled/snowflake" \
    --error-file-path "$ROOT/logs/snowflake_errors.log" \
    --catalog-name    "$CATALOG" \
    --schema-name     "$SCHEMA" \
    --skip-validation false \
    --profile "$PROFILE"

  step "Snowflake transpile complete — review logs/snowflake_errors.log and transpiled/snowflake/"
}

main() {
  case "${1:-all}" in
    install)    cmd_install ;;
    analyze)    cmd_analyze ;;
    transpile)  cmd_transpile ;;
    all)        cmd_install && cmd_analyze && cmd_transpile ;;
    *) echo "usage: $0 [install|analyze|transpile|all]"; exit 2 ;;
  esac
}

main "$@"
