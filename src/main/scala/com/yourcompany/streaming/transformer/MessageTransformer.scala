package com.yourcompany.streaming.transformer

import com.yourcompany.streaming.config.AppConfig
import com.yourcompany.streaming.config.AppConfig.FieldMapping
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Core transformation pipeline:
 *   1. Cast Kafka value bytes → string
 *   2. Parse JSON using schema built from config
 *   3. Flatten nested fields to columns
 *   4. Invoke custom transformer (if configured)
 *   5. Add audit_load_date partition column
 *   6. Validate: null primary keys / partition → bad records
 *   7. Return (goodRecords, badRecords)
 */
object MessageTransformer {

  private val appConfig = AppConfig.application

  // Lazy-load custom transformer via reflection (once)
  private lazy val customTransformer: Option[CustomTransformer] = {
    if (appConfig.hasCustomTransformer) {
      val cls = Class.forName(appConfig.customTransformerClass)
      Some(cls.getDeclaredConstructor().newInstance().asInstanceOf[CustomTransformer])
    } else {
      None
    }
  }

  // Lazy-load custom flattener via reflection (once)
  private lazy val customFlattener: Option[CustomFlattener] = {
    if (appConfig.hasCustomFlattener) {
      val cls = Class.forName(appConfig.customFlattenerClass)
      Some(cls.getDeclaredConstructor().newInstance().asInstanceOf[CustomFlattener])
    } else {
      None
    }
  }

  def transform(rawDf: DataFrame, spark: SparkSession): (DataFrame, DataFrame) = {
    val schema = buildJsonSchema()

    // Step 1-2: bytes → string → parse JSON
    val parsedDf = rawDf
      .selectExpr(
        "CAST(value AS STRING) AS json_raw",
        "topic",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
        "timestamp AS kafka_timestamp"
      )
      .withColumn("parsed", from_json(col("json_raw"), schema))

    // Separate parse failures
    val parseFailures = parsedDf.filter(col("parsed").isNull)
      .select(
        col("json_raw"),
        col("topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        lit("JSON_PARSE_FAILURE").as("error_reason"),
        current_date().as(appConfig.partitionColumn)
      )

    val parsedGood = parsedDf.filter(col("parsed").isNotNull)

    // Step 3: Flatten — use CustomFlattener if provided, otherwise groups-based logic
    var flatDf = customFlattener match {
      case Some(cf) =>
        // Hand off the parsed struct to the custom class — it owns the flatten entirely
        cf.flatten(parsedGood, appConfig.extraParams, spark)
      case None =>
        // Default: drive from config groups
        var df = parsedGood
        for (field <- appConfig.fieldMappings) {
          val sparkPath = "parsed." + field.path
          df = df.withColumn(field.name, col(sparkPath).cast(resolveType(field.dataType)))
        }
        df
    }

    // Step 4: Custom transformer hook
    val targetColumns = customFlattener match {
      case Some(_) => flatDf.columns.toList   // custom flattener owns column list
      case None    => appConfig.fieldMappings.map(_.name)
    }
    var resultDf = flatDf.select(targetColumns.map(col): _*)

    resultDf = customTransformer match {
      case Some(ct) =>
        val transformed = ct.transform(resultDf, spark)
        validateCustomOutput(transformed, targetColumns)
        transformed
      case None =>
        resultDf
    }

    // Step 5: Add partition column
    resultDf = resultDf.withColumn(
      appConfig.partitionColumn,
      date_format(current_timestamp(), appConfig.dateFormat).cast(StringType)
    )

    // Step 6: Validate primary keys not null
    val pkNullCondition = appConfig.primaryKeys
      .map(pk => col(pk).isNull)
      .reduce(_ || _)

    val badPkRecords = resultDf.filter(pkNullCondition)
    val goodRecords = resultDf.filter(!pkNullCondition)

    // Combine all bad records
    // For bad PK records, convert to error format
    val badPkFormatted = badPkRecords
      .withColumn("json_raw", to_json(struct(targetColumns.map(col): _*)))
      .withColumn("topic", lit(AppConfig.kafka.topic))
      .withColumn("kafka_partition", lit(-1))
      .withColumn("kafka_offset", lit(-1L))
      .withColumn("kafka_timestamp", current_timestamp())
      .withColumn("error_reason", lit("NULL_PRIMARY_KEY"))
      .select(
        col("json_raw"),
        col("topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("error_reason"),
        col(appConfig.partitionColumn)
      )

    val allBadRecords = parseFailures.unionByName(badPkFormatted)

    (goodRecords, allBadRecords)
  }

  /**
   * Validates that custom transformer didn't break the contract.
   * Primary keys and expected columns must still exist.
   */
  private def validateCustomOutput(df: DataFrame, expectedColumns: List[String]): Unit = {
    val actualColumns = df.columns.toSet

    // Check primary keys still exist
    val missingPks = appConfig.primaryKeys.filterNot(actualColumns.contains)
    if (missingPks.nonEmpty) {
      throw new IllegalStateException(
        s"Custom transformer dropped primary key column(s): ${missingPks.mkString(", ")}. " +
        s"This breaks the dedup JOIN. Fix your CustomTransformer implementation."
      )
    }
  }

  /**
   * Builds a Spark StructType from config field paths.
   * Handles nested paths like "customer.customer_id" → StructType(customer → StructType(customer_id))
   */
  def buildJsonSchema(): StructType = {
    val paths = appConfig.fieldMappings.map(f => (f.path.split("\\.").toList, f.dataType))
    buildStructType(paths)
  }

  private def buildStructType(paths: List[(List[String], String)]): StructType = {
    val grouped = paths.groupBy(_._1.head)

    val fields = grouped.map { case (key, entries) =>
      val hasLeaf = entries.exists(_._1.length == 1)
      if (hasLeaf) {
        val dataType = entries.find(_._1.length == 1).get._2
        StructField(key, resolveType(dataType), nullable = true)
      } else {
        val childPaths = entries.map { case (parts, dt) => (parts.tail, dt) }
        StructField(key, buildStructType(childPaths), nullable = true)
      }
    }.toSeq

    StructType(fields)
  }

  private def resolveType(typeName: String): DataType = typeName.toLowerCase match {
    case "string"    => StringType
    case "integer"   => IntegerType
    case "int"       => IntegerType
    case "long"      => LongType
    case "double"    => DoubleType
    case "float"     => FloatType
    case "boolean"   => BooleanType
    case "date"      => DateType
    case "timestamp" => TimestampType
    case _           => StringType
  }
}
