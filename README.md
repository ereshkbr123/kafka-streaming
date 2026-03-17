# Kafka → Hive Streaming Framework

A configuration-driven Spark Structured Streaming framework that consumes JSON messages from a Kafka topic, parses and flattens nested fields, applies filters and deduplication, and writes clean records to Hive (production) or Parquet files (local). Bad records are captured to a separate error path for inspection.

---

## Table of Contents

- [Architecture](#architecture)
- [Processing Pipeline](#processing-pipeline)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Configuration Reference](#configuration-reference)
- [Build](#build)
- [Running Locally](#running-locally)
- [Running on a Cluster (Production)](#running-on-a-cluster-production)
- [Custom Transformer](#custom-transformer)
- [Filter Rules](#filter-rules)
- [Deduplication](#deduplication)
- [Error Handling](#error-handling)
- [Output Layout](#output-layout)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

---

## Architecture

```
Kafka Topic
    │
    ▼
KafkaReader          reads raw binary messages (bytes)
    │
    ▼
MessageTransformer
  ├── cast bytes → JSON string
  ├── parse JSON using schema built from config
  ├── flatten nested fields → flat columns
  ├── invoke CustomTransformer (optional)
  ├── add audit_load_date partition column
  └── split → good records / bad records (null PK, parse failures)
    │
    ▼
RecordFilter         drops records that fail config-driven filter rules
    │
    ▼
DedupHandler         LEFT ANTI JOIN against existing target data (lookback window)
    │
    ├──► Writer → Hive table  (enable-hive = true)
    │         or Parquet path  (enable-hive = false)
    │
    └──► Writer → Error Parquet path
```

---

## Processing Pipeline

Each micro-batch goes through these steps in order:

| Step | Component | What it does |
|------|-----------|--------------|
| 1 | `KafkaReader` | Reads raw Kafka messages as a streaming DataFrame |
| 2 | `MessageTransformer` | Parses JSON, flattens nested fields, runs custom transformer, validates primary keys |
| 3 | `RecordFilter` | Keeps only records matching all configured filter rules |
| 4 | `DedupHandler` | Removes records already present in the target (by primary key, over a lookback window) |
| 5 | `Writer` | Appends new records to Hive or Parquet; writes bad records to error path |

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Java | 11 |
| Scala | 2.12.x |
| Apache Spark | 3.4.1 |
| Apache Maven | 3.6+ |
| Apache Kafka | 2.x / 3.x (local: `localhost:9092`) |

**Local Kafka quick-start** (if you don't have one running):

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
bin/kafka-server-start.sh config/server.properties

# Create the topic
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic collections_customer_events \
  --partitions 1 \
  --replication-factor 1
```

---

## Project Structure

```
kafka-hive-streaming/
├── pom.xml
├── src/
│   └── main/
│       ├── resources/
│       │   └── application.conf          # All configuration lives here
│       └── scala/com/example/streaming/
│           ├── app/
│           │   └── StreamingApp.scala    # Entry point (main)
│           ├── config/
│           │   └── AppConfig.scala       # Typesafe Config loader + case classes
│           ├── reader/
│           │   └── KafkaReader.scala     # Spark readStream from Kafka
│           ├── transformer/
│           │   ├── MessageTransformer.scala  # JSON parse, flatten, validate
│           │   └── CustomTransformer.scala   # Trait to extend for custom logic
│           ├── filter/
│           │   └── RecordFilter.scala    # Config-driven row filtering
│           ├── writer/
│           │   └── Writer.scala          # Hive / Parquet writer
│           └── util/
│               ├── DedupHandler.scala    # LEFT ANTI JOIN deduplication
│               └── SparkSessionUtil.scala
└── target/
    └── kafka-hive-streaming-1.0.0.jar   # Fat JAR produced by mvn package
```

---

## Configuration Reference

All settings are in `src/main/resources/application.conf`.

### `spark` block

```hocon
spark {
  app-name          = "collections-customer-events"
  master            = "local[2]"   # "yarn" or "spark://host:7077" on cluster
  enable-hive       = false        # true → write to Hive; false → write to Parquet
  trigger-interval  = "1 minute"   # How often each micro-batch runs
  checkpoint-location = "/tmp/streaming-checkpoints/collections-customer-events"
}
```

| Key | Description |
|-----|-------------|
| `master` | `local[N]` for local, `yarn` / `spark://...` for cluster |
| `enable-hive` | Switches output between Hive table and local Parquet |
| `trigger-interval` | Micro-batch interval (`"1 minute"`, `"30 seconds"`, etc.) |
| `checkpoint-location` | Spark checkpoint directory — must be wiped on schema changes |

---

### `kafka` block

```hocon
kafka {
  bootstrap-servers = "localhost:9092"
  topic             = "collections_customer_events"
  group-id          = "collections-customer-events-cg"
  starting-offsets  = "earliest"   # "latest" to skip historical data
  security-protocol = "PLAINTEXT"  # "SSL" for production

  ssl {
    truststore-location = ""
    truststore-password = ""
    keystore-location   = ""
    keystore-password   = ""
    key-password        = ""
  }
}
```

| Key | Description |
|-----|-------------|
| `starting-offsets` | `"earliest"` replays all history; `"latest"` reads only new messages |
| `security-protocol` | `PLAINTEXT` (local) or `SSL` (production) — SSL options are only applied when protocol contains SSL |

---

### `application` block

```hocon
application {
  hive-database      = "default"
  hive-table         = "collections_customer_events"
  output-path        = "/tmp/streaming-output/collections-customer-events"
  partition-column   = "audit_load_date"
  date-format        = "yyyy-MM-dd"
  primary-keys       = ["event_id"]
  dedup-lookback-days = 2
  custom-transformer-class = ""   # Leave empty if not needed

  json {
    fields = [
      { name = "event_id",       path = "event_id",                   type = "string"  }
      { name = "customer_id",    path = "customer.customer_id",        type = "string"  }
      { name = "loan_amount",    path = "loan_account.loan_amount",    type = "integer" }
      # ... add more fields here
    ]
  }
}
```

#### JSON field mappings

Each entry under `json.fields` maps a JSON path to a flat output column:

| Key | Description |
|-----|-------------|
| `name` | Output column name in the target table |
| `path` | Dot-separated path in the source JSON (e.g. `customer.address.city`) |
| `type` | Target data type: `string`, `integer`, `int`, `long`, `double`, `float`, `boolean`, `date`, `timestamp` |

**Example — mapping a 3-level nested field:**

```hocon
{ name = "city", path = "customer.address.city", type = "string" }
```

#### Supported input JSON structure (example)

```json
{
  "event_id": "EVT-001",
  "event_timestamp": "2026-03-16T10:00:00Z",
  "customer": {
    "customer_id": "C-123",
    "first_name": "Jane",
    "last_name": "Smith",
    "risk_rating": "LOW"
  },
  "contact": {
    "email": "jane@example.com",
    "phone_mobile": "+1-555-0100"
  },
  "address": {
    "city": "Austin",
    "state": "TX",
    "country": "US"
  },
  "loan_account": {
    "account_id": "ACC-456",
    "loan_amount": 25000,
    "interest_rate": 5.75
  },
  "delinquency": {
    "days_past_due": 30,
    "bucket": "30-60"
  }
}
```

---

### `filter` block

```hocon
filter {
  rules = [
    { field = "event_id",    operator = "is_not_null" }
    { field = "customer_id", operator = "is_not_null" }
  ]
}
```

All rules are combined with AND logic — a record must pass every rule to be kept. See [Filter Rules](#filter-rules) for all supported operators.

---

### `error` block

```hocon
error {
  error-path           = "/tmp/streaming-output/collections-customer-events-errors"
  enable-error-capture = true
}
```

---

## Build

```bash
# Requires Java 11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11

mvn package
```

This produces two JARs in `target/`:

| JAR | Description |
|-----|-------------|
| `kafka-hive-streaming-1.0.0.jar` | **Fat JAR** — use this for `spark-submit` (includes Kafka connector and Typesafe Config) |
| `original-kafka-hive-streaming-1.0.0.jar` | Thin JAR without shaded dependencies |

Spark, Scala, and Hive dependencies are marked `provided` and are not bundled — they are expected from the Spark runtime.

---

## Running Locally

### 1. Clear checkpoints (required after config/schema changes)

```bash
rm -rf /tmp/streaming-checkpoints/collections-customer-events
```

### 2. Submit

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@11 \
  spark-submit \
  --master "local[2]" \
  --class com.example.streaming.app.StreamingApp \
  target/kafka-hive-streaming-1.0.0.jar
```

### 3. Verify output

```bash
# Good records (Parquet, partitioned by date)
ls /tmp/streaming-output/collections-customer-events/

# Error records
ls /tmp/streaming-output/collections-customer-events-errors/

# Read with Spark shell or Python
spark.read.parquet("/tmp/streaming-output/collections-customer-events").show()
```

### 4. Produce test messages

```bash
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic collections_customer_events
```

Paste a JSON message and press Enter. The next micro-batch (within `trigger-interval`) will process it.

---

## Running on a Cluster (Production)

### 1. Update `application.conf`

```hocon
spark {
  master           = "yarn"
  enable-hive      = true
  trigger-interval = "15 minutes"
  checkpoint-location = "hdfs:///checkpoints/collections-customer-events"
}

kafka {
  bootstrap-servers = "broker1:9092,broker2:9092,broker3:9092"
  starting-offsets  = "latest"
  security-protocol = "SSL"

  ssl {
    truststore-location = "/etc/kafka/ssl/truststore.jks"
    truststore-password = "changeit"
    keystore-location   = "/etc/kafka/ssl/keystore.jks"
    keystore-password   = "changeit"
    key-password        = "changeit"
  }
}

application {
  hive-database = "collections"
  hive-table    = "customer_events"
  output-path   = "hdfs:///data/collections/customer_events"
}
```

### 2. Submit to YARN

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-cores 2 \
  --executor-memory 4g \
  --driver-memory 2g \
  --class com.example.streaming.app.StreamingApp \
  kafka-hive-streaming-1.0.0.jar
```

### 3. Create the Hive target table (first time only)

```sql
CREATE TABLE collections.customer_events (
  event_id        STRING,
  event_timestamp STRING,
  customer_id     STRING,
  first_name      STRING,
  last_name       STRING,
  risk_rating     STRING,
  email           STRING,
  phone_mobile    STRING,
  city            STRING,
  state           STRING,
  country         STRING,
  account_id      STRING,
  loan_amount     INT,
  interest_rate   DOUBLE,
  days_past_due   INT,
  bucket          STRING
)
PARTITIONED BY (audit_load_date STRING)
STORED AS PARQUET;
```

The column order in the `CREATE TABLE` must match the order of fields defined in `application.conf → json.fields`, with the partition column last.

---

## Custom Transformer

For business logic that cannot be expressed in config (derived columns, complex lookups, data masking, etc.), implement the `CustomTransformer` trait.

### Step 1 — Implement the trait

```scala
package com.example

import com.example.streaming.transformer.CustomTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class MyTransformer extends CustomTransformer {

  override def transform(df: DataFrame, spark: SparkSession): DataFrame = {
    df
      // Mask PII
      .withColumn("email", lit("***MASKED***"))
      // Derive a new column
      .withColumn("risk_score", when(col("risk_rating") === "HIGH", 3)
                                  .when(col("risk_rating") === "MEDIUM", 2)
                                  .otherwise(1))
  }
}
```

**Rules:**
- Do NOT drop or rename primary key columns (breaks dedup JOIN)
- You may add columns, modify values, or filter rows
- The framework validates that primary keys still exist after your transform

### Step 2 — Build a JAR for your transformer

```bash
mvn package -f your-transformer-pom.xml
```

### Step 3 — Configure

```hocon
application {
  custom-transformer-class = "com.example.MyTransformer"
}
```

### Step 4 — Submit with the extra JAR

```bash
spark-submit \
  --jars your-custom-transformer-1.0.0.jar \
  --class com.example.streaming.app.StreamingApp \
  kafka-hive-streaming-1.0.0.jar
```

---

## Filter Rules

Configured under the `filter.rules` list. All rules are ANDed — a record must satisfy every rule to pass through.

| Operator | Required keys | Example |
|----------|---------------|---------|
| `is_not_null` | `field` | `{ field = "event_id", operator = "is_not_null" }` |
| `is_null` | `field` | `{ field = "closed_date", operator = "is_null" }` |
| `equals` | `field`, `value` | `{ field = "country", operator = "equals", value = "US" }` |
| `not_equals` | `field`, `value` | `{ field = "status", operator = "not_equals", value = "DELETED" }` |
| `in` | `field`, `values` | `{ field = "bucket", operator = "in", values = ["30-60","60-90"] }` |
| `not_in` | `field`, `values` | `{ field = "risk_rating", operator = "not_in", values = ["EXCLUDED"] }` |

**Example — keep only active US customers:**

```hocon
filter {
  rules = [
    { field = "event_id",    operator = "is_not_null" }
    { field = "customer_id", operator = "is_not_null" }
    { field = "country",     operator = "equals",     value  = "US" }
    { field = "risk_rating", operator = "not_in",     values = ["EXCLUDED", "FRAUD"] }
  ]
}
```

---

## Deduplication

The framework deduplicates each micro-batch against data already written to the target. This prevents duplicate records even if Kafka delivers a message more than once or the job is restarted.

**How it works:**

1. Reads the target (Hive table or Parquet path) for the last `dedup-lookback-days` days
2. Selects only the primary key columns + partition column (efficient, no full scan)
3. Performs a `LEFT ANTI JOIN` — keeps only records from the incoming batch whose primary key is not found in the existing data

**Configuration:**

```hocon
application {
  primary-keys        = ["event_id"]          # Can be composite: ["order_id", "line_id"]
  dedup-lookback-days = 2                     # How many days of target to scan
}
```

**Notes:**
- On first run (no target data yet), all incoming records are treated as new
- Increase `dedup-lookback-days` if late-arriving duplicates can span more than 2 days
- Composite primary keys are supported — list all key columns

---

## Error Handling

Records that fail processing are written to the error path instead of being dropped silently.

**Error types captured:**

| `error_reason` | Cause |
|----------------|-------|
| `JSON_PARSE_FAILURE` | Kafka message could not be parsed as valid JSON matching the configured schema |
| `NULL_PRIMARY_KEY` | Record parsed successfully but one or more primary key columns are null |

**Error record schema:**

| Column | Description |
|--------|-------------|
| `json_raw` | The original raw JSON string |
| `topic` | Kafka topic name |
| `kafka_partition` | Kafka partition number |
| `kafka_offset` | Kafka offset of the message |
| `kafka_timestamp` | Kafka message timestamp |
| `error_reason` | One of the error types above |
| `audit_load_date` | Date partition |

**Reading error records:**

```python
# PySpark / Spark SQL
errors = spark.read.parquet("/tmp/streaming-output/collections-customer-events-errors")
errors.filter("error_reason = 'JSON_PARSE_FAILURE'").show(truncate=False)
```

---

## Output Layout

### Good records (local / Parquet mode)

```
/tmp/streaming-output/collections-customer-events/
└── audit_load_date=2026-03-16/
    └── part-00000-<uuid>.snappy.parquet
```

### Error records

```
/tmp/streaming-output/collections-customer-events-errors/
└── audit_load_date=2026-03-16/
    └── part-00000-<uuid>.snappy.parquet
```

### Checkpoints

```
/tmp/streaming-checkpoints/collections-customer-events/
├── commits/
├── metadata
├── offsets/
└── sources/
```

> **Important:** Delete the checkpoint directory when you change the schema (add/remove fields), change `starting-offsets`, or make incompatible config changes. Stale checkpoints will cause the job to fail or read from wrong offsets.

---

## Monitoring

### Spark UI

The Spark UI is available at `http://localhost:4040` (or 4041/4042 if ports are taken). Check:

- **Streaming** tab — batch durations, input rows/sec, processing rates
- **SQL** tab — query plans per micro-batch
- **Jobs / Stages** — execution details

### Batch log output

```
=== Streaming Framework Starting ===
  App:        collections-customer-events
  Topic:      collections_customer_events
  Target:     /tmp/streaming-output/collections-customer-events
  Trigger:    1 minute
  Dedup keys: event_id
  Lookback:   2 days

=== Streaming query active. Waiting for data... ===

--- Batch 0 (2154 raw messages) ---
  After filter: 2154 records
  After dedup:  2154 new records
  Batch 0: Wrote 2154 records
--- Batch 0 complete ---
```

### Monitor live logs

```bash
# If running as a background process, tail the output file
tail -f <spark-submit-log> | grep -E "(Batch|filter|dedup|ERROR|WARN)"
```

---

## Troubleshooting

### Job fails immediately with `AnalysisException: Table not found`

- First run with `enable-hive = true` and the Hive table doesn't exist yet. Create the table first (see [Create the Hive target table](#3-create-the-hive-target-table-first-time-only)).
- In local mode this is expected on the first run — `DedupHandler` catches it and treats all records as new.

### `Checkpoint incompatible` or schema mismatch on restart

Delete the checkpoint directory and restart:

```bash
rm -rf /tmp/streaming-checkpoints/collections-customer-events
```

### Kafka connection refused

```
org.apache.kafka.common.errors.TimeoutException: Topic not present in metadata
```

- Verify Kafka is running: `nc -z localhost 9092`
- Verify the topic exists: `kafka-topics.sh --list --bootstrap-server localhost:9092`

### All records go to the error table

- Check `error_reason`. If `JSON_PARSE_FAILURE` — the message structure doesn't match the `json.fields` config.
- If `NULL_PRIMARY_KEY` — the field mapped to your primary key (`event_id` by default) is missing or null in the source message.
- Use `kafka-console-consumer` to inspect raw messages: `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic collections_customer_events --from-beginning --max-messages 5`

### High dedup overhead / slow batches

- Reduce `dedup-lookback-days` if 2-day lookback is wider than needed.
- Ensure the partition column filter prunes partitions effectively.
- On Hive, make sure partition statistics are up to date: `ANALYZE TABLE ... PARTITION(...)`.

### Custom transformer breaks the job

```
IllegalStateException: Custom transformer dropped primary key column(s): event_id
```

Your `CustomTransformer.transform` removed or renamed a primary key column. Ensure the columns listed in `primary-keys` are preserved in the returned DataFrame.

---

## Quick Reference

```bash
# Build
JAVA_HOME=/opt/homebrew/opt/openjdk@11 mvn package

# Clear checkpoint and run locally
rm -rf /tmp/streaming-checkpoints/collections-customer-events
JAVA_HOME=/opt/homebrew/opt/openjdk@11 \
  /path/to/spark-3.4.1/bin/spark-submit \
  --master "local[2]" \
  --class com.example.streaming.app.StreamingApp \
  target/kafka-hive-streaming-1.0.0.jar

# Produce a test message
echo '{"event_id":"E1","event_timestamp":"2026-03-16T10:00:00Z","customer":{"customer_id":"C1","first_name":"Jane","last_name":"Smith","risk_rating":"LOW"},"contact":{"email":"jane@example.com","phone_mobile":"+1-555-0100"},"address":{"city":"Austin","state":"TX","country":"US"},"loan_account":{"account_id":"A1","loan_amount":25000,"interest_rate":5.75},"delinquency":{"days_past_due":0,"bucket":"current"}}' \
  | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic collections_customer_events

# Read output
spark-shell --master local
scala> spark.read.parquet("/tmp/streaming-output/collections-customer-events").show()
```

## Spark Structured Streaming vs DStreams — Why Structured Streaming?
    1. No more RDD-to-DataFrame conversion every batch.
    
    
    2. Catalyst optimizer works on the entire pipeline.
       In DStreams, the optimizer only kicks in after you've manually created the DataFrame from the RDD. 
       In Structured Streaming, the optimizer sees the full plan — from Kafka read through your from_json, through the flatten, through the filter, through the JOIN, all the way to the write. 
       It can push predicates down, prune columns early, and optimize the JOIN plan holistically.
    
    3. maxOffsetsPerTrigger — built-in rate limiting.
    
    Say one of your 200 teams has a pipeline that's been down for 2 days and restarts. 
    With DStreams, that first batch pulls everything — potentially millions of messages — and can OOM or overwhelm the cluster. 
    You've probably built defensive code around this. Structured Streaming gives you:
    
    kafka {
    max-offsets-per-trigger = 100000
    }
    
    4. StreamingQueryListener — framework-level monitoring.
    
    5. foreachBatch is a first-class contract.
          In DStreams, your foreachRDD works, but it's an RDD-level callback. foreachBatch gives you a proper DataFrame with schema, 
          with the optimizer already applied, with all Kafka metadata columns (partition, offset, timestamp) intact.
    
