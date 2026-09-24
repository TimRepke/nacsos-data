#!/usr/bin/env bash

set -euo pipefail

# Default configuration values
SOURCE_DIR="."
DB_NAME="nacsos_core_project"
DB_USER="nacsos"
DB_HOST="localhost"
DB_PORT="5432"
PGPASSFILE=""

# Print usage instructions
usage() {
    cat << EOF
Usage: $0 --source-dir <id> [options]

Required:
  -o, --source-dir DIR  Source export directory (default: .)

Options:
  -d, --db-name NAME    Database name (default: nacsos_core_project)
  -u, --user USERNAME   Database user (default: nacsos)
  -H, --host HOSTNAME   Database host (default: localhost)
  -p, --port PORT       Database port (default: 5432)
  -c, --pgpass          pgpass file
  -h, --help            Show this help message
EOF
    exit "${1:-0}"
}

# Parse named parameters using standard getopt
PARSED_ARGS=$(getopt -o "o:d:u:H:p:c:h" --long "source-dir:,db-name:,user:,host:,port:,pgpass:,help" -- "$@") || exit 1
eval set -- "$PARSED_ARGS"

while true; do
    case "$1" in
        -o|--source-dir) SOURCE_DIR="$2"; shift 2 ;;
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

# Resolve absolute path to export.sql (located alongside this script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/import.sql"

if [ ! -f "$SQL_FILE" ]; then
    echo "Error: Cannot find SQL script at $SQL_FILE"
    exit 1
fi

# Change into the newly created folder
cd "$SOURCE_DIR"

# Build psql arguments dynamically based on provided credentials
PSQL_CONN=()
[ -n "$DB_USER" ] && PSQL_CONN+=("-U" "$DB_USER")
[ -n "$DB_HOST" ] && PSQL_CONN+=("-h" "$DB_HOST")
[ -n "$DB_PORT" ] && PSQL_CONN+=("-p" "$DB_PORT")

export PGPASSFILE="$PGPASSFILE"

echo "--> Creating new database '${DB_NAME}'..."
createdb "${PSQL_CONN[@]}" "$DB_NAME"
PSQL_CONN+=("-d" "$DB_NAME")

echo "--> Creating schema..."
psql "${PSQL_CONN[@]}" -f schema.sql

echo "--> Importing data..."
psql "${PSQL_CONN[@]}" -f "${SQL_FILE}"

echo "--> Success! Exported files saved in: $(pwd)"