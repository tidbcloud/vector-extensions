#!/usr/bin/env sh
# Parse Vector raw_log_source config (raw_logs: start_time, end_time, raw_log_components) and sink
# key_prefix fixed part; run one aws s3 sync per (hour, component) for progress.
# Path rule (see path_resolver): diagnosis/data/{cluster_id}/merged-logs/{YYYYMMDDHH}/{component}/

set -e

CONFIG_FILE="${CONFIG_FILE:-/config/vector.toml}"
SYNC_EXTRA_ARGS="${SYNC_EXTRA_ARGS:-}"
AWS_EXTRA_ARGS="${AWS_EXTRA_ARGS:-}"

# Extract TOML: from YAML ConfigMap or use file as TOML.
get_toml_content() {
    if grep -q "vector.toml:" "$1" 2>/dev/null; then
        sed -n '/vector.toml: *|/,/^  [a-zA-Z]/p' "$1" | sed '1d' | sed '/^  [a-zA-Z]/d' | sed 's/^    //'
    else
        cat "$1"
    fi
}

# Get scalar value in a TOML section.
get_toml_value() {
    local content="$1"
    local section="$2"
    local key="$3"
    local section_prefix=""
    case "$section" in
        sources.raw_log_source) section_prefix="[sources.raw_log_source]" ;;
        sinks.to_s3)      section_prefix="[sinks.to_s3]" ;;
        *) section_prefix="[$section]" ;;
    esac
    local in_section=0
    echo "$content" | while IFS= read -r line; do
        line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        if [ "$line" = "$section_prefix" ]; then
            in_section=1
            continue
        fi
        if [ "$in_section" = 1 ] && [ -n "$line" ] && echo "$line" | grep -q '^\['; then
            break
        fi
        if [ "$in_section" = 1 ] && echo "$line" | grep -q "^${key}[[:space:]]*="; then
            echo "$line" | sed -n "s/^${key}[[:space:]]*=[[:space:]]*//p" | sed 's/^"\(.*\)"$/\1/;s/^'"'"'\(.*\)'"'"'$/\1/'
            break
        fi
    done
}

# Parse raw_log_components = [ "a", "b", "c" ] into one component per line.
get_toml_array_values() {
    local content="$1"
    local section="$2"
    local key="$3"
    local section_prefix=""
    case "$section" in
        sources.raw_log_source) section_prefix="[sources.raw_log_source]" ;;
        *) section_prefix="[$section]" ;;
    esac
    local in_section=0
    local line_content
    echo "$content" | while IFS= read -r line; do
        line_content=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        if [ "$line_content" = "$section_prefix" ]; then
            in_section=1
            continue
        fi
        if [ "$in_section" = 1 ] && [ -n "$line_content" ] && echo "$line_content" | grep -q '^\['; then
            break
        fi
        if [ "$in_section" = 1 ] && echo "$line_content" | grep -q "^${key}[[:space:]]*="; then
            echo "$line_content" | sed -n "s/^${key}[[:space:]]*=[[:space:]]*//p" | sed 's/^\[//;s/\]//' | tr ',' '\n' | sed 's/^[[:space:]]*"//;s/"[[:space:]]*$//;s/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v '^$'
            break
        fi
    done
}

# key_prefix fixed part: before first "{{" (e.g. "leotest6/{{ component }}/..." -> "leotest6")
key_prefix_fixed() {
    echo "$1" | sed 's|{{.*||' | sed 's|/*$||'
}

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file not found: $CONFIG_FILE" >&2
    exit 1
fi

TOML_CONTENT=$(get_toml_content "$CONFIG_FILE")

# Source: endpoint is s3://bucket or s3://bucket/prefix
ENDPOINT=$(get_toml_value "$TOML_CONTENT" "sources.raw_log_source" "endpoint")
BUCKET=$(get_toml_value "$TOML_CONTENT" "sinks.to_s3" "bucket")
KEY_PREFIX_RAW=$(get_toml_value "$TOML_CONTENT" "sinks.to_s3" "key_prefix")
REGION=$(get_toml_value "$TOML_CONTENT" "sinks.to_s3" "region")
[ -z "$REGION" ] && REGION=$(get_toml_value "$TOML_CONTENT" "sources.raw_log_source" "region")

# raw_log_source raw_logs
CLUSTER_ID=$(get_toml_value "$TOML_CONTENT" "sources.raw_log_source" "cluster_id")
START_TIME=$(get_toml_value "$TOML_CONTENT" "sources.raw_log_source" "start_time")
END_TIME=$(get_toml_value "$TOML_CONTENT" "sources.raw_log_source" "end_time")
TYPES=$(get_toml_value "$TOML_CONTENT" "sources.raw_log_source" "types")
COMPONENTS=$(get_toml_array_values "$TOML_CONTENT" "sources.raw_log_source" "raw_log_components")

if [ -z "$ENDPOINT" ] || [ -z "$BUCKET" ]; then
    echo "Missing [sources.raw_log_source] endpoint or [sinks.to_s3] bucket" >&2
    exit 1
fi
# endpoint s3://bucket or s3://bucket/prefix -> bucket name only (path after bucket is implied by diagnosis/...)
# We use same bucket for source; path is diagnosis/data/{cluster_id}/merged-logs/...
case "$ENDPOINT" in
    s3://*) S3_BUCKET=$(echo "$ENDPOINT" | sed 's|s3://||' | cut -d/ -f1) ;;
    *) echo "Unsupported endpoint: $ENDPOINT" >&2; exit 1 ;;
esac

if [ -z "$CLUSTER_ID" ] || [ -z "$START_TIME" ] || [ -z "$END_TIME" ]; then
    echo "raw_logs requires cluster_id, start_time, end_time in [sources.raw_log_source]" >&2
    exit 1
fi
if ! echo "$TYPES" | grep -q "raw_logs"; then
    echo "Only types = [ \"raw_logs\" ] is supported" >&2
    exit 1
fi
if [ -z "$COMPONENTS" ]; then
    echo "raw_log_components must be non-empty" >&2
    exit 1
fi

DEST_PREFIX=$(key_prefix_fixed "$KEY_PREFIX_RAW")
[ -z "$DEST_PREFIX" ] && DEST_PREFIX="backup"

# Generate hourly timestamps from start to end (inclusive). Truncate to hour. GNU date (Amazon Linux).
start_epoch=$(date -u -d "$START_TIME" +%s)
end_epoch=$(date -u -d "$END_TIME" +%s)
start_hr_epoch=$(date -u -d "$(date -u -d "@$start_epoch" +%Y-%m-%dT%H:00:00Z)" +%s)
end_hr_epoch=$(date -u -d "$(date -u -d "@$end_epoch" +%Y-%m-%dT%H:00:00Z)" +%s)

HOURS=""
t=$start_hr_epoch
while [ "$t" -le "$end_hr_epoch" ]; do
    HOURS="$HOURS $(date -u -d "@$t" +%Y%m%d%H)"
    t=$((t + 3600))
done

AWS_CMD="aws"
[ -n "$REGION" ] && AWS_CMD="$AWS_CMD --region $REGION"
[ -n "$AWS_EXTRA_ARGS" ] && AWS_CMD="$AWS_CMD $AWS_EXTRA_ARGS"

# One sync per (hour, component): source diagnosis/data/{cluster_id}/merged-logs/{YYYYMMDDHH}/{component}/ -> dest {dest_prefix}/{component}/{YYYYMMDDHH}/
total=0
for hour in $HOURS; do
    for comp in $COMPONENTS; do
        total=$((total + 1))
    done
done
n=0
for hour in $HOURS; do
    for comp in $COMPONENTS; do
        n=$((n + 1))
        SOURCE="s3://${S3_BUCKET}/diagnosis/data/${CLUSTER_ID}/merged-logs/${hour}/${comp}/"
        DEST="s3://${BUCKET}/${DEST_PREFIX}/${comp}/${hour}/"
        echo "[$n/$total] sync $hour / $comp"
        eval "$AWS_CMD s3 sync \"$SOURCE\" \"$DEST\" $SYNC_EXTRA_ARGS"
    done
done
echo "Done. Synced $total prefix(es)."
