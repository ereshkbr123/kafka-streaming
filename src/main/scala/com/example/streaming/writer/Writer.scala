package com.example.streaming.writer

import com.example.streaming.config.AppConfig
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Writes data to target (Hive or Parquet) and errors to error path/table.
 * Coalesces to avoid small files.
 */
object Writer {

  private val appConfig   = AppConfig.application
  private val errorConfig = AppConfig.error

  def writeGoodRecords(df: DataFrame, batchId: Long): Unit = {
    if (df.isEmpty) {
      println(s"  Batch $batchId: No new records to write")
      return
    }

    val count = df.count()

    if (AppConfig.spark.enableHive) {
      // Production: insertInto Hive
      // Column order must match table: data columns first, partition column last
      val columnOrder = appConfig.fieldMappings.map(_.name) :+ appConfig.partitionColumn
      df.select(columnOrder.map(df.col): _*)
        .coalesce(1)
        .write
        .mode(SaveMode.Append)
        .insertInto(appConfig.fullTableName)
    } else {
      // Local: write partitioned Parquet
      df.coalesce(1)
        .write
        .mode(SaveMode.Append)
        .partitionBy(appConfig.partitionColumn)
        .parquet(appConfig.outputPath)
    }

    println(s"  Batch $batchId: Wrote $count records")
  }

  def writeBadRecords(df: DataFrame, batchId: Long): Unit = {
    if (!errorConfig.enableErrorCapture) return
    if (df.isEmpty) return

    val count = df.count()

    df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .partitionBy(appConfig.partitionColumn)
      .parquet(errorConfig.errorPath)

    println(s"  Batch $batchId: Wrote $count error records")
  }
}
