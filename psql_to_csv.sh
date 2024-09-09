psql_to_csv() {
    local db="$1"
    local table_name="$2"
    local output_file="$3"
    local host="${4-localhost}"
    local user="${5-postgres}"
    local port="${6-5432}"

    psql -h "$host" -U "$user" -p "$port" -d "$db" -c "\copy (SELECT * FROM $table_name) TO '$output_file' WITH (FORMAT CSV, HEADER);"
}



