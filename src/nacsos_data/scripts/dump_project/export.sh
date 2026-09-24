#!/usr/bin/env bash

set -euo pipefail
#set -x
# Default configuration values
PROJECT_ID=""
TARGET_DIR="."
DB_NAME="nacsos_core"
DB_USER="nacsos"
DB_HOST="localhost"
DB_PORT="5432"
PGPASSFILE=""

# Print usage instructions
usage() {
    cat << EOF
Usage: $0 --project-id <id> [options]

Required:
  -i, --project-id ID   Project ID to export

Options:
  -o, --target-dir DIR  Target output directory (default: .)
  -d, --db-name NAME    Database name (default: nacsos_core)
  -u, --user USERNAME   Database user (default: nacsos)
  -H, --host HOSTNAME   Database host
  -p, --port PORT       Database port
  -c, --pgpass          pgpass file
  -h, --help            Show this help message
EOF
    exit "${1:-0}"
}

# Parse named parameters using standard getopt
PARSED_ARGS=$(getopt -o "i:o:d:u:H:p:c:h" --long "project-id:,target-dir:,db-name:,user:,host:,port:,pgpass:,help" -- "$@") || exit 1
eval set -- "$PARSED_ARGS"

while true; do
    case "$1" in
        -i|--project-id) PROJECT_ID="$2"; shift 2 ;;
        -o|--target-dir) TARGET_DIR="$2"; shift 2 ;;
        -d|--db-name)    DB_NAME="$2"; shift 2 ;;
        -u|--user)       DB_USER="$2"; shift 2 ;;
        -H|--host)       DB_HOST="$2"; shift 2 ;;
        -p|--port)       DB_PORT="$2"; shift 2 ;;
        -c|--pgpass)     PGPASSFILE="$(readlink -f "$2")"; shift 2 ;;
        -h|--help)       usage 0 ;;
        --)              shift; break ;;
        *)               echo "Error: Unexpected option $1"; usage 1 ;;
    esac
done

# Check for required Project ID parameter
if [ -z "$PROJECT_ID" ]; then
    echo "Error: Missing required parameter --project-id."
    usage 1
fi

# Resolve absolute path to export.sql (located alongside this script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/export.sql"

if [ ! -f "$SQL_FILE" ]; then
    echo "Error: Cannot find SQL script at $SQL_FILE"
    exit 1
fi

# Construct target folder: <target_dir>/YYYYMMDD_projectID
FOLDER_NAME="$(date +%Y%m%d)_${PROJECT_ID}"
EXPORT_PATH="${TARGET_DIR}/${FOLDER_NAME}"

echo "--> Creating output directory: ${EXPORT_PATH}"
mkdir -p "$EXPORT_PATH"

# Change into the newly created folder
cd "$EXPORT_PATH"

# Build psql arguments dynamically based on provided credentials
PSQL_CONN=("-d" "$DB_NAME")
PSQL_ARGS=("-v" "project_id=$PROJECT_ID" "-f" "$SQL_FILE")

[ -n "$DB_USER" ] && PSQL_CONN+=("-U" "$DB_USER")
[ -n "$DB_HOST" ] && PSQL_CONN+=("-h" "$DB_HOST")
[ -n "$DB_PORT" ] && PSQL_CONN+=("-p" "$DB_PORT")
PSQL_ARGS=("${PSQL_CONN[@]}" "${PSQL_ARGS[@]}")

export PGPASSFILE="$PGPASSFILE"

# Execute psql
echo "--> Starting export for Project ID: ${PROJECT_ID}..."
psql "${PSQL_ARGS[@]}"

echo "--> Dumping schema..."
pg_dump -s "${PSQL_CONN[@]}" --no-privileges -f schema.sql

echo "--> Success! Exported files saved in: $(pwd)"