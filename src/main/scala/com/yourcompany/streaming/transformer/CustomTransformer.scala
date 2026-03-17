package com.yourcompany.streaming.transformer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Framework contract for teams with custom transformation logic.
 *
 * Usage:
 *   1. Create a class extending this trait
 *   2. Implement the transform method
 *   3. Build a jar
 *   4. Set custom-transformer-class in application.conf
 *   5. Pass the jar with --jars in spark-submit
 *
 * Rules:
 *   - Do NOT drop or rename primary key columns
 *   - Do NOT drop or rename the partition column
 *   - You may add new columns, modify values, filter rows
 *   - The returned DataFrame will be validated by the framework
 */
trait CustomTransformer extends Serializable {

  def transform(df: DataFrame, spark: SparkSession): DataFrame
}
