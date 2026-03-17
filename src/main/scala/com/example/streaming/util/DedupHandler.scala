package com.example.streaming.util

import com.example.streaming.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Deduplicates incoming batch against existing target data.
 *
 * Reads target table/path for last N days (configurable).
 * LEFT ANTI JOIN on primary keys → keeps only new records.
 *
 * This is the framework's source of truth for exactly-once.
 * Not checkpoints, not offsets — the data itself.
 */
object DedupHandler {

  private val appConfig = AppConfig.application

  def dedup(incomingDf: DataFrame, spark: SparkSession): DataFrame = {
    val existingDf = readExistingData(spark)

    if (existingDf.isEmpty) {
      // First run or no data in lookback window — everything is new
      incomingDf
    } else {
      // LEFT ANTI JOIN: keep records from incoming that do NOT exist in target
      val joinKeys = appConfig.primaryKeys
      val joinCondition = joinKeys
        .map(key => incomingDf(key) === existingDf(key))
        .reduce(_ && _)

      incomingDf.join(existingDf, joinCondition, "left_anti")
    }
  }

  /**
   * Reads existing data from target for the lookback window.
   * Supports both Hive table (prod) and Parquet path (local).
   */
  private def readExistingData(spark: SparkSession): DataFrame = {
    try {
      val lookbackDays = appConfig.dedupLookbackDays
      val partitionCol = appConfig.partitionColumn

      val baseDf = if (AppConfig.spark.enableHive) {
        // Production: read from Hive table
        spark.table(appConfig.fullTableName)
      } else {
        // Local testing: read from Parquet path
        spark.read.parquet(appConfig.outputPath)
      }

      // Filter to lookback window
      // Only read primary key columns + partition column for efficiency
      val selectCols = appConfig.primaryKeys :+ partitionCol

      baseDf
        .filter(
          col(partitionCol).between(
            date_format(date_sub(current_date(), lookbackDays), appConfig.dateFormat),
            date_format(current_date(), appConfig.dateFormat)
          )
        )
        .select(selectCols.map(col): _*)

    } catch {
      // First run — target doesn't exist yet
      case _: org.apache.spark.sql.AnalysisException =>
        spark.emptyDataFrame
    }
  }
}
