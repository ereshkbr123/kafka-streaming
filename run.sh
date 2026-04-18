#!/bin/bash
# =============================================================================
# Kafka → Hive Streaming Framework — run script
#
# Usage:
#   ./run.sh <environment> <conf-file> <truststore-password> <keystore-password> <key-password>
#
# Examples:
#   ./run.sh dev  src/main/resources/dev/application.conf  TrustPass KeyPass KeyPass
#   ./run.sh uat  src/main/resources/uat/application.conf  TrustPass KeyPass KeyPass
#   ./run.sh prod src/main/resources/prod/application.conf TrustPass KeyPass KeyPass
#
# Config contract:
#   Each conf file must contain a shell-vars { } block at column 1 and may
#   contain a spark-confs = [ ] array at column 1. Both are parsed by the
#   functions below using only grep and sed — no awk, jq, python, or JVM helper.
# =============================================================================

ENV=$1
CONF_FILE=$2
TRUSTSTORE_PASSWORD=$3
KEYSTORE_PASSWORD=$4
KEY_PASSWORD=$5

# =============================================================================
# load_shell_conf <conf-file>
#
# Parses the shell-vars { } block from the HOCON conf file and exports each
# key as an UPPER_SNAKE_CASE environment variable in the current shell.
#
# Defensive requirements met:
#   - Hard-fails if file missing, block missing, or any line is malformed.
#   - Validates each line with a regex before touching any variable.
#   - Rejects values containing $, backtick, or backslash (injection guard).
#   - Refuses keys that map to reserved shell/OS variable names.
#   - Uses heredoc-fed while loop (NOT pipe-to-while) so exports survive the loop.
# =============================================================================
load_shell_conf() {
    local conf_file="$1"

    # Hard-fail if the file does not exist — no silent fallbacks
    if [ ! -f "$conf_file" ]; then
        echo "ERROR [load_shell_conf]: conf file not found: $conf_file" >&2
        exit 1
    fi

    # Extract the body of the shell-vars block.
    #
    # Three-pass sed pipeline — written this way for BSD sed (macOS) compatibility.
    # BSD sed does not support semicolons to separate commands inside range-pattern
    # braces (GNU sed does). Splitting into separate passes avoids that portability trap.
    #
    # Pass 1: sed -n + range print  — extracts the shell-vars block INCLUDING the
    #         opening "shell-vars {" line and closing "}" line.
    # Pass 2: sed '1d'              — removes the first line (opening brace line).
    # Pass 3: sed '$d'              — removes the last line (closing brace line).
    # Result: only the key = "value" body lines remain.
    local block
    block=$(sed -n '/^shell-vars[[:space:]]*{/,/^}/p' "$conf_file" | sed '1d' | sed '$d')

    if [ -z "$block" ]; then
        echo "ERROR [load_shell_conf]: shell-vars block not found or empty in: $conf_file" >&2
        exit 1
    fi

    # Reserved variable names we must never clobber.
    # These are relied on by the OS, login shell, and JVM launcher at startup.
    # LC_* is checked separately via a prefix case match below.
    local reserved_list="PATH HOME IFS PS1 PS2 SHELL USER PWD LANG TERM LD_LIBRARY_PATH JAVA_HOME HADOOP_HOME SPARK_HOME"

    # Use a heredoc-fed while loop instead of:  echo "$block" | while read ...
    #
    # Reason: a pipe creates a subshell for the right-hand side. Any 'export'
    # inside that subshell is invisible to the parent shell — variables would
    # be set and then silently discarded when the subshell exits. The heredoc
    # form feeds stdin to a while loop in the CURRENT shell, so exports persist.
    while IFS= read -r line; do

        # Skip blank lines
        [ -z "$line" ] && continue

        # Skip comment-only lines.
        # Strip leading whitespace first, then test if the first char is '#'.
        # Using 'case' instead of grep to avoid spawning a subshell per iteration.
        local stripped_line
        stripped_line=$(printf '%s' "$line" | sed 's/^[[:space:]]*//')
        case "$stripped_line" in
            '#'*) continue ;;
        esac

        # Validate line format: key = "value"
        #
        # Regex breakdown (POSIX ERE via grep -E):
        #   ^[[:space:]]*              optional leading whitespace (indentation)
        #   [a-zA-Z][a-zA-Z0-9_-]*    key: must start with a letter, then
        #                              alphanumeric / underscore / hyphen
        #   [[:space:]]*=[[:space:]]*  equals sign with optional surrounding spaces
        #   ".*"                       value: double-quoted string (any interior)
        #   [[:space:]]*$              optional trailing whitespace
        #
        # grep -qE: quiet mode (no output to stdout), extended regex, exit 1 if no match.
        if ! printf '%s' "$line" | grep -qE '^[[:space:]]*[a-zA-Z][a-zA-Z0-9_-]*[[:space:]]*=[[:space:]]*".*"[[:space:]]*$'; then
            echo "ERROR [load_shell_conf]: malformed line (expected  key = \"value\"):" >&2
            echo "  $line" >&2
            exit 1
        fi

        # Extract the raw key: everything left of '=', whitespace stripped.
        # Three sed passes kept separate for readability and POSIX portability
        # (chaining with -e is portable, but separate pipes are easier to audit).
        local raw_key
        raw_key=$(printf '%s' "$line" \
            | sed 's/[[:space:]]*=.*//' \
            | sed 's/^[[:space:]]*//' \
            | sed 's/[[:space:]]*$//')

        # Extract the raw value: everything right of '=', whitespace stripped,
        # then the surrounding double-quotes removed.
        local raw_val
        raw_val=$(printf '%s' "$line" \
            | sed 's/[^=]*=[[:space:]]*//' \
            | sed 's/^"//' \
            | sed 's/"[[:space:]]*$//')

        # Injection guard: reject values containing $, backtick, or backslash.
        #
        # Even though we export with 'export KEY=value' (not eval), a value
        # containing these characters could cause harm if the variable is later
        # used in an unquoted context or passed to a command that processes it.
        # shell-vars values are paths and identifiers — none of these chars are
        # legitimate there.
        case "$raw_val" in
            *'$'*)
                echo "ERROR [load_shell_conf]: '\$' not allowed in shell-vars value for key '$raw_key'" >&2
                exit 1
                ;;
            *'`'*)
                echo "ERROR [load_shell_conf]: backtick not allowed in shell-vars value for key '$raw_key'" >&2
                exit 1
                ;;
            *'\'*)
                echo "ERROR [load_shell_conf]: backslash not allowed in shell-vars value for key '$raw_key'" >&2
                exit 1
                ;;
        esac

        # Convert kebab-case key to UPPER_SNAKE_CASE.
        #
        # Two separate tr calls — NOT one combined call — because combining
        # character-class translation and literal-char substitution in a single
        # tr invocation has divergent behaviour between BSD tr (macOS) and GNU tr
        # (Linux). Keeping them separate is unambiguous on both.
        #
        #   tr '-' '_'              : replace every hyphen with underscore
        #   tr '[:lower:]' '[:upper:]' : uppercase every ASCII letter
        local env_key
        env_key=$(printf '%s' "$raw_key" | tr '-' '_' | tr '[:lower:]' '[:upper:]')

        # Reserved-name guard: exact match against the known reserved list.
        local is_reserved=0
        for r in $reserved_list; do
            if [ "$env_key" = "$r" ]; then
                is_reserved=1
                break
            fi
        done

        # LC_* prefix guard: any locale-category variable is off-limits —
        # overwriting LC_ALL or LC_CTYPE mid-script can corrupt string handling.
        case "$env_key" in
            LC_*) is_reserved=1 ;;
        esac

        if [ "$is_reserved" -eq 1 ]; then
            echo "ERROR [load_shell_conf]: key '$raw_key' maps to reserved var '$env_key' — rename it in shell-vars" >&2
            exit 1
        fi

        # Export the variable.
        # Using the 'export KEY=value' form (not eval) prevents code execution:
        # the value is treated as a literal string, not a shell expression.
        export "$env_key=$raw_val"

    done <<EOF
$block
EOF
}

# =============================================================================
# load_spark_confs <conf-file>
#
# Parses the spark-confs = [ ] array from the HOCON conf file and populates
# the global bash array SPARK_CONFS_ARR with alternating "--conf" / "key=value"
# pairs ready for direct expansion into spark-submit.
#
# A missing spark-confs block is not an error — returns 0 with empty array.
# =============================================================================
load_spark_confs() {
    local conf_file="$1"

    # Initialise as a global bash array (visible outside this function).
    # Declared without 'local' so it persists after the function returns.
    SPARK_CONFS_ARR=()

    # If no spark-confs key exists at column 1, the block is absent.
    # Not all envs need extra confs — return silently with an empty array.
    if ! grep -q '^spark-confs[[:space:]]*=' "$conf_file" 2>/dev/null; then
        return 0
    fi

    # Extract the array body.
    #
    # Same three-pass BSD-safe sed pipeline as load_shell_conf.
    # Pass 1: extract from "spark-confs = [" to the first "]" at column 1.
    # Pass 2: remove the opening line.
    # Pass 3: remove the closing line.
    local block
    block=$(sed -n '/^spark-confs[[:space:]]*=[[:space:]]*\[/,/^\]/p' "$conf_file" | sed '1d' | sed '$d')

    if [ -z "$block" ]; then
        # Key present but array body is empty — nothing to add
        return 0
    fi

    while IFS= read -r line; do

        [ -z "$line" ] && continue

        # Skip comment-only lines
        local stripped_line
        stripped_line=$(printf '%s' "$line" | sed 's/^[[:space:]]*//')
        case "$stripped_line" in
            '#'*) continue ;;
        esac

        # Validate: each element must be a quoted string.
        # Optional trailing comma is permitted (some HOCON style guides include it).
        #
        # Regex:  ^[[:space:]]*"[^"]*"[,]?[[:space:]]*$
        #   [^"]*  — any chars except double-quote (no nested quotes allowed here)
        #   [,]?   — optional trailing comma before close of line
        if ! printf '%s' "$line" | grep -qE '^[[:space:]]*"[^"]*"[,]?[[:space:]]*$'; then
            echo "ERROR [load_spark_confs]: malformed array element (expected  \"spark.key=value\"):" >&2
            echo "  $line" >&2
            exit 1
        fi

        # Extract value: strip leading whitespace, leading quote, trailing quote + optional comma.
        local val
        val=$(printf '%s' "$line" \
            | sed 's/^[[:space:]]*//' \
            | sed 's/^"//' \
            | sed 's/"[,]*[[:space:]]*$//')

        # Each spark conf entry must contain '=' to be a valid key=value pair.
        case "$val" in
            *'='*) ;;  # valid
            *)
                echo "ERROR [load_spark_confs]: spark conf entry missing '=' separator: $val" >&2
                exit 1
                ;;
        esac

        # Reject shell command substitution forms.
        #
        # We allow bare '$' and '${VAR}' because JVM extraJavaOptions legitimately
        # use those for property references (e.g. ${spark.executor.memory}).
        # We block only the forms that cause the SHELL to execute a command:
        #   backtick  `cmd`  and  dollar-paren  $(cmd)
        case "$val" in
            *'`'*)
                echo "ERROR [load_spark_confs]: backtick command substitution not allowed: $val" >&2
                exit 1
                ;;
            *'$('*)
                echo "ERROR [load_spark_confs]: \$() command substitution not allowed: $val" >&2
                exit 1
                ;;
        esac

        # Append two entries: the flag and its value as separate array elements.
        #
        # Why two entries instead of one "--conf key=value" string?
        # When expanded as "${SPARK_CONFS_ARR[@]}", bash treats each array element
        # as its own word. A single "--conf key=value" element would be passed to
        # spark-submit as ONE argument, which it would reject. Two elements produce
        # the correct argument list:  spark-submit ... --conf key=value ...
        SPARK_CONFS_ARR+=("--conf")
        SPARK_CONFS_ARR+=("$val")

    done <<EOF
$block
EOF
}

# =============================================================================
# Validate inputs
# =============================================================================
if [ -z "$ENV" ]; then
    echo "ERROR: environment argument is required"
    echo "Usage: ./run.sh <env> <conf-file> <truststore-password> <keystore-password> <key-password>"
    exit 1
fi

if [ -z "$CONF_FILE" ]; then
    echo "ERROR: conf-file argument is required"
    echo "Usage: ./run.sh <env> <conf-file> <truststore-password> <keystore-password> <key-password>"
    exit 1
fi

if [ ! -f "$CONF_FILE" ]; then
    echo "ERROR: conf file not found: $CONF_FILE"
    exit 1
fi

if [ -z "$TRUSTSTORE_PASSWORD" ] || [ -z "$KEYSTORE_PASSWORD" ] || [ -z "$KEY_PASSWORD" ]; then
    echo "ERROR: all three passwords are required"
    echo "Usage: ./run.sh <env> <conf-file> <truststore-password> <keystore-password> <key-password>"
    exit 1
fi

# =============================================================================
# Load config from the chosen env file
# =============================================================================
load_shell_conf  "$CONF_FILE"   # exports CHECKPOINT_LOCATION, KAFKA_BROKERS,
                                # YARN_QUEUE, TRUSTSTORE_LOCATION, KEYSTORE_LOCATION,
                                # SPARK_MAIN_CLASS, SPARK_JAR_PATH, DEPLOY_MODE
load_spark_confs "$CONF_FILE"   # populates SPARK_CONFS_ARR global array

# =============================================================================
# Show resolved shell-vars
# Every variable exported by load_shell_conf is printed here so you can verify
# what was parsed from the conf file before anything is executed.
# =============================================================================
echo "========================================"
echo " Environment        : $ENV"
echo " Conf file          : $CONF_FILE"
echo " CHECKPOINT_LOCATION: $CHECKPOINT_LOCATION"
echo " KAFKA_BROKERS      : $KAFKA_BROKERS"
echo " YARN_QUEUE         : $YARN_QUEUE"
echo " TRUSTSTORE_LOCATION: $TRUSTSTORE_LOCATION"
echo " KEYSTORE_LOCATION  : $KEYSTORE_LOCATION"
echo " SPARK_MAIN_CLASS   : $SPARK_MAIN_CLASS"
echo " SPARK_JAR_PATH     : $SPARK_JAR_PATH"
echo " DEPLOY_MODE        : $DEPLOY_MODE"
echo "========================================"

# Show the parsed spark-confs array — each --conf pair on its own line.
if [ "${#SPARK_CONFS_ARR[@]}" -gt 0 ]; then
    echo " spark-confs:"
    i=0
    while [ "$i" -lt "${#SPARK_CONFS_ARR[@]}" ]; do
        # Elements are stored as pairs: [--conf] [key=value]
        # Print only the key=value element (every odd index) for readability
        echo "   --conf ${SPARK_CONFS_ARR[$((i+1))]}"
        i=$((i + 2))
    done
else
    echo " spark-confs: (none)"
fi
echo "========================================"

# =============================================================================
# Show the exact spark-submit command that will be executed.
# Passwords are redacted so this output is safe to log or share.
# =============================================================================
echo ""
echo ">>> spark-submit command (passwords redacted):"
echo ""
echo "spark-submit \\"
echo "  --master yarn \\"
echo "  --deploy-mode \"$DEPLOY_MODE\" \\"
echo "  --queue \"$YARN_QUEUE\" \\"
echo "  --class \"$SPARK_MAIN_CLASS\" \\"
echo "  --driver-java-options \"-DENV=$ENV -Dconfig.file=$CONF_FILE \\"
echo "    -Dssl.truststore.location=$TRUSTSTORE_LOCATION \\"
echo "    -Dssl.keystore.location=$KEYSTORE_LOCATION \\"
echo "    -Dssl.truststore.password=*** \\"
echo "    -Dssl.keystore.password=*** \\"
echo "    -Dssl.key.password=***\" \\"
echo "  --conf \"spark.executor.extraJavaOptions= \\"
echo "    -Dssl.truststore.location=$TRUSTSTORE_LOCATION \\"
echo "    -Dssl.keystore.location=$KEYSTORE_LOCATION \\"
echo "    -Dssl.truststore.password=*** \\"
echo "    -Dssl.keystore.password=*** \\"
echo "    -Dssl.key.password=***\" \\"
# Print each --conf pair from SPARK_CONFS_ARR
i=0
while [ "$i" -lt "${#SPARK_CONFS_ARR[@]}" ]; do
    echo "  --conf \"${SPARK_CONFS_ARR[$((i+1))]}\" \\"
    i=$((i + 2))
done
echo "  \"$SPARK_JAR_PATH\""
echo ""

# =============================================================================
# DRY_RUN mode: set DRY_RUN=1 to print the command without executing it.
#
#   DRY_RUN=1 ./run.sh dev src/main/resources/dev/application.conf x x x
#
# Useful for verifying conf parsing and command construction without needing
# a Spark cluster or valid passwords.
# =============================================================================
if [ "${DRY_RUN:-0}" = "1" ]; then
    echo "[DRY_RUN] Skipping checkpoint clear and spark-submit."
    exit 0
fi

# =============================================================================
# Clear checkpoint directory
# Checkpoint location comes from $CHECKPOINT_LOCATION (loaded above).
# =============================================================================
if [ -n "$CHECKPOINT_LOCATION" ]; then
    echo "Clearing checkpoint: $CHECKPOINT_LOCATION"
    rm -rf "$CHECKPOINT_LOCATION"
fi

# =============================================================================
# Run spark-submit
#
# -DENV=$ENV
#   Passes the environment name as a JVM system property so HOCON base config
#   can reference ${?ENV} in substitutions if needed later.
#
# --conf "spark.executor.extraJavaOptions=..."
#   Replaces the previous --executor-java-options flag, which is not a valid
#   spark-submit argument.
#
# "${SPARK_CONFS_ARR[@]}"
#   Double-quoted array expansion: each element is its own word, preserving
#   spaces inside values. Expands to nothing when the array is empty.
# =============================================================================
spark-submit \
  --master yarn \
  --deploy-mode "$DEPLOY_MODE" \
  --queue "$YARN_QUEUE" \
  --class "$SPARK_MAIN_CLASS" \
  --driver-java-options "-DENV=$ENV -Dconfig.file=$CONF_FILE -Dssl.truststore.location=$TRUSTSTORE_LOCATION -Dssl.keystore.location=$KEYSTORE_LOCATION -Dssl.truststore.password=$TRUSTSTORE_PASSWORD -Dssl.keystore.password=$KEYSTORE_PASSWORD -Dssl.key.password=$KEY_PASSWORD" \
  --conf "spark.executor.extraJavaOptions=-Dssl.truststore.location=$TRUSTSTORE_LOCATION -Dssl.keystore.location=$KEYSTORE_LOCATION -Dssl.truststore.password=$TRUSTSTORE_PASSWORD -Dssl.keystore.password=$KEYSTORE_PASSWORD -Dssl.key.password=$KEY_PASSWORD" \
  "${SPARK_CONFS_ARR[@]}" \
  "$SPARK_JAR_PATH"

EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "ERROR: Job failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi
echo "Job submitted successfully"