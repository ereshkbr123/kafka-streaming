package com.yourcompany.streaming.app

import com.yourcompany.streaming.config.AppConfig
import com.yourcompany.streaming.filter.RecordFilter
import com.yourcompany.streaming.reader.KafkaReader
import com.yourcompany.streaming.transformer.MessageTransformer
import com.yourcompany.streaming.util.{DedupHandler, SparkSessionUtil}
import com.yourcompany.streaming.writer.Writer
import org.apache.spark.sql.streaming.Trigger

/**
 * Kafka → Parse → Custom Transform → Filter → Dedup → Hive/Parquet
 *                                         ↘ Bad records → Error table
 *
 * This is the framework entry point.
 * Teams configure via application.conf, optionally provide custom transformer jar.
 */
object StreamingApp {

  def main(args: Array[String]): Unit = {

    val cfg = AppConfig.spark
    val kafkaCfg = AppConfig.kafka
    val appCfg = AppConfig.application

    println("=== Streaming Framework Starting ===")
    println(s"  App:        ${cfg.appName}")
    println(s"  Topic:      ${kafkaCfg.topic}")
    println(s"  Target:     ${if (cfg.enableHive) appCfg.fullTableName else appCfg.outputPath}")
    println(s"  Trigger:    ${cfg.triggerInterval}")
    println(s"  Dedup keys: ${appCfg.primaryKeys.mkString(", ")}")
    println(s"  Lookback:   ${appCfg.dedupLookbackDays} days")
    if (appCfg.hasCustomTransformer) {
      println(s"  Custom:     ${appCfg.customTransformerClass}")
    }

    // 1. Spark session
    val spark = SparkSessionUtil.create()

    // 2. Read Kafka stream
    val rawStream = KafkaReader.read(spark)

    // 3. Process micro-batches
    val query = rawStream.writeStream
      .trigger(Trigger.ProcessingTime(cfg.triggerInterval))
      .option("checkpointLocation", cfg.checkpointLocation)
      .foreachBatch { (batchDf: org.apache.spark.sql.DataFrame, batchId: Long) =>

        if (!batchDf.isEmpty) {
          println(s"\n--- Batch $batchId (${batchDf.count()} raw messages) ---")

          // 3a. Parse JSON + flatten + custom transform + validate
          val (goodRecords, badRecords) = MessageTransformer.transform(batchDf, spark)

          // 3b. Apply config-driven filters
          val filteredRecords = RecordFilter.apply(goodRecords)
          println(s"  After filter: ${filteredRecords.count()} records")

          // 3c. Dedup against existing target data
          val newRecords = DedupHandler.dedup(filteredRecords, spark)
          println(s"  After dedup:  ${newRecords.count()} new records")

          // 3d. Write
          Writer.writeGoodRecords(newRecords, batchId)
          Writer.writeBadRecords(badRecords, batchId)

          println(s"--- Batch $batchId complete ---")
        }
      }
      .start()

    println("\n=== Streaming query active. Waiting for data... ===\n")
    query.awaitTermination()
  }
}
