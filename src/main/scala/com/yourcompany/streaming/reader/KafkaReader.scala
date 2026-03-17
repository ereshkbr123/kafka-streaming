package com.yourcompany.streaming.reader

import com.yourcompany.streaming.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaReader {

  def read(spark: SparkSession): DataFrame = {
    val cfg = AppConfig.kafka

    var reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrapServers)
      .option("subscribe", cfg.topic)
      .option("startingOffsets", cfg.startingOffsets)
      .option("kafka.group.id", cfg.groupId)
      .option("failOnDataLoss", "false")
      .option("kafka.security.protocol", cfg.securityProtocol)

    // Apply SSL options only when protocol requires it
    if (cfg.securityProtocol.toUpperCase.contains("SSL")) {
      reader = reader
        .option("kafka.ssl.truststore.location", cfg.ssl.truststoreLocation)
        .option("kafka.ssl.truststore.password", cfg.ssl.truststorePassword)
        .option("kafka.ssl.keystore.location", cfg.ssl.keystoreLocation)
        .option("kafka.ssl.keystore.password", cfg.ssl.keystorePassword)
        .option("kafka.ssl.key.password", cfg.ssl.keyPassword)
    }

    reader.load()
  }
}
