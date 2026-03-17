package com.yourcompany.streaming.util

import com.yourcompany.streaming.config.AppConfig
import org.apache.spark.sql.SparkSession

object SparkSessionUtil {

  def create(): SparkSession = {
    val cfg = AppConfig.spark

    val builder = SparkSession.builder()
      .appName(cfg.appName)
      .master(cfg.master)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.shuffle.partitions", "10")
      // Local warehouse directory for non-Hive mode
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")

    if (cfg.enableHive) {
      builder.enableHiveSupport()
    }

    builder.getOrCreate()
  }
}
