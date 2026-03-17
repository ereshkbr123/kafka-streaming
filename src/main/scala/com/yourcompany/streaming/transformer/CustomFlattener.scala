package com.yourcompany.streaming.transformer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Optional hook to replace the framework's groups-based flattening entirely.
 *
 * Usage:
 *   1. Create a class extending this trait
 *   2. Implement flatten() — receive raw parsed JSON DataFrame, return flat DataFrame
 *   3. Set custom-flatten-class in application.conf
 *   4. Access any extra config via extraParams map
 *
 * Rules:
 *   - Input df has one column: "parsed" (the full JSON as a Spark struct)
 *   - Output df must contain all primary key columns
 *   - Do NOT include the partition column — the framework adds it after
 *
 * Example:
 *   class MyFlattener extends CustomFlattener {
 *     def flatten(df: DataFrame, extraParams: Map[String, String], spark: SparkSession): DataFrame = {
 *       val threshold = extraParams.getOrElse("threshold", "0").toInt
 *       df.select(
 *         col("parsed.event_id"),
 *         col("parsed.customer.customer_id"),
 *         col("parsed.loan_account.loan_amount").cast(IntegerType)
 *       ).filter(col("loan_amount") > threshold)
 *     }
 *   }
 */
trait CustomFlattener extends Serializable {

  /**
   * @param df          DataFrame with "parsed" struct column (full JSON already parsed by Spark)
   * @param extraParams All key-value pairs from application.extra-params block in config
   * @param spark       Active SparkSession
   * @return            Flat DataFrame — one column per target field
   */
  def flatten(df: DataFrame, extraParams: Map[String, String], spark: SparkSession): DataFrame
}