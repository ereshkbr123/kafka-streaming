# Kafka → Hive Streaming Framework — User Guide

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Directory Layout](#2-directory-layout)
3. [Running the Script](#3-running-the-script)
4. [Dry-Run Mode (verify before executing)](#4-dry-run-mode-verify-before-executing)
5. [Preparing a Conf File](#5-preparing-a-conf-file)
   - [File structure](#51-file-structure)
   - [shell-vars block rules](#52-shell-vars-block-rules)
   - [shell-vars key reference](#53-shell-vars-key-reference)
   - [spark-confs block rules](#54-spark-confs-block-rules)
   - [JVM-side overrides](#55-jvm-side-overrides)
6. [Adding a New Environment](#6-adding-a-new-environment)
7. [Common Errors and Fixes](#7-common-errors-and-fixes)

---

## 1. Prerequisites

| Requirement | Notes |
|-------------|-------|
| `bash` 3.2+ | macOS default is 3.2; Linux is typically 4+ |
| `spark-submit` on `PATH` | Must be the version matching your cluster (3.4.x) |
| `grep`, `sed` | Standard POSIX — present on every Linux/macOS host |
| JKS keystores | Must exist at the paths you specify in `shell-vars` |
| Built JAR | `kafka-hive-streaming-1.0.0.jar` in the working directory (or set `spark-jar-path` to the full path) |

---

## 2. Directory Layout

```
kafka-streaming/
├── run.sh                                  ← the script you run
├── src/main/resources/
│   ├── application-base.conf               ← shared config (spark, kafka, application, filter, error)
│   ├── application.conf                    ← original local/dev conf (kept for local Spark testing)
│   ├── dev/
│   │   └── application.conf               ← DEV environment conf
│   ├── uat/
│   │   └── application.conf               ← UAT environment conf
│   └── prod/
│       └── application.conf               ← PROD environment conf
```

Each env conf file:
- Pulls shared config via `include classpath("application-base.conf")`
- Overrides env-specific Spark and Kafka settings for the JVM
- Contains a `shell-vars` block parsed by `run.sh`
- Contains a `spark-confs` array parsed by `run.sh`

---

## 3. Running the Script

### Syntax

```bash
./run.sh <env> <conf-file> <truststore-password> <keystore-password> <key-password>
```

| Argument | Description |
|----------|-------------|
| `<env>` | Environment label printed in logs (e.g. `dev`, `uat`, `prod`) |
| `<conf-file>` | Path to the environment conf file |
| `<truststore-password>` | Password for the Kafka SSL truststore JKS |
| `<keystore-password>` | Password for the Kafka SSL keystore JKS |
| `<key-password>` | Password for the private key inside the keystore |

### Examples

```bash
# DEV
./run.sh dev src/main/resources/dev/application.conf TrustPass KeyPass KeyPass

# UAT
./run.sh uat src/main/resources/uat/application.conf TrustPass KeyPass KeyPass

# PROD
./run.sh prod src/main/resources/prod/application.conf TrustPass KeyPass KeyPass
```

### What happens when you run it

1. `load_shell_conf` reads the `shell-vars { }` block from your conf file and exports every key as an environment variable (`CHECKPOINT_LOCATION`, `KAFKA_BROKERS`, etc.).
2. `load_spark_confs` reads the `spark-confs = [ ]` array and builds a list of `--conf key=value` pairs.
3. The script prints all resolved variables and the full spark-submit command (with passwords redacted).
4. The checkpoint directory is wiped (`rm -rf`).
5. `spark-submit` is called with the assembled arguments.

---

## 4. Dry-Run Mode (verify before executing)

Set `DRY_RUN=1` to see every resolved variable and the exact spark-submit command that **would** be run — without clearing the checkpoint or submitting anything.

```bash
DRY_RUN=1 ./run.sh dev src/main/resources/dev/application.conf x x x
```

You can use placeholder passwords (`x x x`) in dry-run because they are never passed to a real cluster.

### Sample dry-run output

```
========================================
 Environment        : dev
 Conf file          : src/main/resources/dev/application.conf
 CHECKPOINT_LOCATION: /tmp/streaming-checkpoints/kafka-to-hive-transmit
 KAFKA_BROKERS      : dev-broker1:9093,dev-broker2:9093
 YARN_QUEUE         : default
 TRUSTSTORE_LOCATION: /etc/kafka/ssl/dev/truststore.jks
 KEYSTORE_LOCATION  : /etc/kafka/ssl/dev/keystore.jks
 SPARK_MAIN_CLASS   : com.yourcompany.streaming.app.StreamingApp
 SPARK_JAR_PATH     : kafka-hive-streaming-1.0.0.jar
 DEPLOY_MODE        : cluster
========================================
 spark-confs:
   --conf spark.sql.adaptive.enabled=true
   --conf spark.sql.shuffle.partitions=200
========================================

>>> spark-submit command (passwords redacted):

spark-submit \
  --master yarn \
  --deploy-mode "cluster" \
  --queue "default" \
  --class "com.yourcompany.streaming.app.StreamingApp" \
  --driver-java-options "-DENV=dev -Dconfig.file=src/main/resources/dev/application.conf ..." \
  --conf "spark.executor.extraJavaOptions=..." \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.shuffle.partitions=200" \
  "kafka-hive-streaming-1.0.0.jar"

[DRY_RUN] Skipping checkpoint clear and spark-submit.
```

---

## 5. Preparing a Conf File

### 5.1 File structure

Every environment conf file must follow this structure (order matters for readability; HOCON itself does not enforce order):

```hocon
include classpath("application-base.conf")   # 1. pull shared config

spark {                                       # 2. override env-specific Spark keys
    master              = "yarn"
    checkpoint-location = "/your/checkpoint/path"
}

kafka {                                       # 3. override env-specific Kafka keys
    bootstrap-servers = "broker1:9093,broker2:9093"
    security-protocol = "SSL"
}

shell-vars { ... }                            # 4. bash-parseable block (see §5.2)

spark-confs = [ ... ]                         # 5. extra --conf pairs (see §5.4)
```

> **Why `include classpath(...)`?**  
> The conf file lives in a subdirectory (`dev/`, `uat/`, `prod/`).  
> A bare `include "application-base.conf"` would look in that same subdirectory and fail.  
> The `classpath()` form tells HOCON to look at the JVM classpath root (`src/main/resources/`), where `application-base.conf` lives.

---

### 5.2 shell-vars block rules

The `shell-vars { }` block is read by **both** the JVM (Typesafe Config) and `run.sh` (bash).  
The bash parser is intentionally simple — it uses only `grep` and `sed`.  
**You must follow these rules exactly or the script will hard-fail with a clear error.**

| Rule | Correct | Wrong |
|------|---------|-------|
| One key per line | `checkpoint-location = "/path"` | `a = "1" b = "2"` on same line |
| Values must be double-quoted | `yarn-queue = "default"` | `yarn-queue = default` |
| No HOCON substitutions | `kafka-brokers = "broker1:9093"` | `kafka-brokers = ${brokers}` |
| No nested objects | flat keys only | `ssl { location = "..." }` |
| Keys use kebab-case | `kafka-brokers` | `kafkaBrokers` or `kafka_brokers` |
| Comments on own line only | `# this is a comment` on its own line | `yarn-queue = "uat" # comment` |
| Opening brace at column 1 | `shell-vars {` starts at position 0 | `  shell-vars {` (indented) |
| Closing brace at column 1 | `}` starts at position 0 | `  }` (indented) |
| No `$`, backtick, or `\` in values | `"/etc/kafka/ssl/dev/truststore.jks"` | `"$HOME/truststore.jks"` |

**Template:**

```hocon
# =============================================================================
# SHELL-VARIABLES BLOCK — parsed by run.sh AND JVM (Typesafe Config)
# =============================================================================
shell-vars {
    checkpoint-location  = "/your/checkpoint/path"
    kafka-brokers        = "broker1:9093,broker2:9093"
    yarn-queue           = "your-queue"
    truststore-location  = "/path/to/truststore.jks"
    keystore-location    = "/path/to/keystore.jks"
    spark-main-class     = "com.yourcompany.streaming.app.StreamingApp"
    spark-jar-path       = "kafka-hive-streaming-1.0.0.jar"
    deploy-mode          = "cluster"
}
```

---

### 5.3 shell-vars key reference

| Key | Exported as | Description |
|-----|-------------|-------------|
| `checkpoint-location` | `$CHECKPOINT_LOCATION` | Spark checkpoint directory. Wiped on every restart. |
| `kafka-brokers` | `$KAFKA_BROKERS` | Comma-separated broker list. Printed in the run summary. |
| `yarn-queue` | `$YARN_QUEUE` | YARN queue passed to `--queue`. |
| `truststore-location` | `$TRUSTSTORE_LOCATION` | Path to the JKS truststore on cluster nodes. Passed via `-Dssl.truststore.location`. |
| `keystore-location` | `$KEYSTORE_LOCATION` | Path to the JKS keystore on cluster nodes. Passed via `-Dssl.keystore.location`. |
| `spark-main-class` | `$SPARK_MAIN_CLASS` | Fully-qualified main class passed to `--class`. |
| `spark-jar-path` | `$SPARK_JAR_PATH` | Path to the application JAR (relative or absolute). |
| `deploy-mode` | `$DEPLOY_MODE` | `cluster` or `client`. Passed to `--deploy-mode`. |

> **Key naming:** kebab-case in the conf file (`kafka-brokers`) becomes UPPER\_SNAKE\_CASE in bash (`$KAFKA_BROKERS`). The conversion is: replace `-` with `_`, then uppercase all letters.

---

### 5.4 spark-confs block rules

The `spark-confs` array holds extra `--conf key=value` pairs that are appended to `spark-submit`. Each entry becomes two arguments: `--conf` and `key=value`.

**Rules:**

| Rule | Correct | Wrong |
|------|---------|-------|
| HOCON array of strings | `spark-confs = ["spark.key=val"]` | `spark-confs { key = val }` |
| Each element is `"spark.key=value"` | `"spark.executor.memoryOverhead=2g"` | `"--conf spark.executor.memoryOverhead=2g"` (no `--conf` prefix) |
| Must contain `=` | `"spark.sql.shuffle.partitions=200"` | `"spark.sql.shuffle.partitions"` |
| No backtick or `$()` | `"spark.driver.memory=4g"` | `"spark.driver.memory=$(cat file)"` |
| Opening bracket on same line as key | `spark-confs = [` at column 1 | opening bracket on next line |
| Closing bracket at column 1 | `]` alone on its own line | `]` indented |

**Template:**

```hocon
# =============================================================================
# SPARK-CONFS — extra --conf key=value pairs for spark-submit
# =============================================================================
spark-confs = [
    "spark.sql.adaptive.enabled=true"
    "spark.sql.shuffle.partitions=200"
    "spark.executor.memoryOverhead=2g"
]
```

> **Duplication note:** `application-base.conf` also defines a `spark-confs` array with common entries. Because `run.sh` parses only the text of the chosen env file, you must **repeat the base entries** in your env file. The env file's array completely replaces the base array at runtime from bash's perspective.

---

### 5.5 JVM-side overrides

Beyond `shell-vars` and `spark-confs`, your env file should override any base config keys that differ per environment. The JVM reads these via Typesafe Config.

**Keys you will almost always override:**

```hocon
spark {
    master              = "yarn"                              # base default: local[2]
    checkpoint-location = "/checkpoints/myenv/my-pipeline"   # must match shell-vars value
}

kafka {
    bootstrap-servers = "broker1:9093,broker2:9093"          # base default: localhost:9092
    security-protocol = "SSL"                                 # base default: PLAINTEXT
}
```

**Keys you may optionally override** (if they differ from the base):

```hocon
application {
    hive-database = "my_db"
    hive-table    = "my_table"
}

error {
    error-path = "/errors/myenv/my-pipeline"
}
```

---

## 6. Adding a New Environment

To add a new environment (e.g. `sit`):

**Step 1** — Create the directory and conf file:

```bash
mkdir -p src/main/resources/sit
cp src/main/resources/dev/application.conf src/main/resources/sit/application.conf
```

**Step 2** — Edit `src/main/resources/sit/application.conf`:

- Update the `spark` override block (checkpoint path)
- Update the `kafka` override block (brokers)
- Fill in all eight `shell-vars` keys with SIT-specific values
- Fill in `spark-confs` with the complete list of confs you need (including any base entries you want to pass)

**Step 3** — Verify with dry-run:

```bash
DRY_RUN=1 ./run.sh sit src/main/resources/sit/application.conf x x x
```

**Step 4** — Run for real:

```bash
./run.sh sit src/main/resources/sit/application.conf TrustPass KeyPass KeyPass
```

No changes to `run.sh` or any Scala source are required.

---

## 7. Common Errors and Fixes

### `ERROR [load_shell_conf]: conf file not found`

The path you passed as `<conf-file>` does not exist.

```
# Wrong — running from wrong directory
./run.sh dev application.conf x x x

# Right — path relative to where you run the script
./run.sh dev src/main/resources/dev/application.conf x x x
```

---

### `ERROR [load_shell_conf]: shell-vars block not found or empty`

The conf file exists but has no `shell-vars {` block starting at column 1.

Check that:
- The block exists in the file
- `shell-vars {` is at column 1 (no leading spaces)
- The closing `}` is at column 1

---

### `ERROR [load_shell_conf]: malformed line`

A line in `shell-vars` does not match the expected format `key = "value"`.

Common causes:

| Cause | Example bad line | Fix |
|-------|-----------------|-----|
| Unquoted value | `yarn-queue = default` | `yarn-queue = "default"` |
| Inline comment | `yarn-queue = "uat" # comment` | Move comment to its own line |
| HOCON substitution | `kafka-brokers = ${brokers}` | Use a literal value |
| Nested object | `ssl { location = "..." }` | Flat keys only in shell-vars |

---

### `ERROR [load_shell_conf]: '$' not allowed in shell-vars value`

You used a shell or HOCON variable reference inside a shell-vars value. These must be literal strings — no variable expansion.

```hocon
# Wrong
checkpoint-location = "$HOME/checkpoints"

# Right
checkpoint-location = "/home/myuser/checkpoints"
```

---

### `ERROR [load_spark_confs]: malformed array element`

An entry in `spark-confs` is not a quoted string, or contains characters the parser rejects.

```hocon
# Wrong — missing quotes
spark-confs = [
    spark.executor.memory=4g
]

# Wrong — has --conf prefix (run.sh adds it)
spark-confs = [
    "--conf spark.executor.memory=4g"
]

# Right
spark-confs = [
    "spark.executor.memory=4g"
]
```

---

### `ERROR [load_spark_confs]: spark conf entry missing '=' separator`

An element in `spark-confs` is a valid quoted string but does not contain `=`.

```hocon
# Wrong
spark-confs = [
    "spark.executor.memory"
]

# Right
spark-confs = [
    "spark.executor.memory=4g"
]
```

---

### JVM fails with `com.typesafe.config.ConfigException$Missing`

A key that the Scala code reads is missing from the loaded config.  
Most likely causes:

1. **`include classpath("application-base.conf")` is missing** from the env file — the shared config was never loaded.
2. **A key that lives in the base was accidentally removed** from `application-base.conf`.
3. **`-Dconfig.file` was not passed** to the JVM — Typesafe Config loaded `application.conf` from the classpath instead of your env file, and that file is missing the key.

Verify which file the JVM loaded by adding `-Dconfig.trace=loads` to `--driver-java-options`.

---

### Checkpoint not being cleared

The `rm -rf` step only runs when `$CHECKPOINT_LOCATION` is non-empty. If it is empty, `load_shell_conf` would have already failed. If the path is wrong, check the `checkpoint-location` value in `shell-vars` and verify it matches the actual `spark.checkpoint-location` override in the same file.