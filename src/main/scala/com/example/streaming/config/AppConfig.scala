package com.example.streaming.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

object AppConfig {

  private lazy val rootConfig: Config = ConfigFactory.load()

  lazy val spark: SparkConfig             = loadSparkConfig()
  lazy val kafka: KafkaConfig             = loadKafkaConfig()
  lazy val application: ApplicationConfig = loadApplicationConfig()
  lazy val filter: FilterConfig           = loadFilterConfig()
  lazy val error: ErrorConfig             = loadErrorConfig()

  // --- Case Classes ---

  case class SparkConfig(
    appName: String,
    master: String,
    enableHive: Boolean,
    triggerInterval: String,
    checkpointLocation: String
  )

  case class SslConfig(
    truststoreLocation: String,
    truststorePassword: String,
    keystoreLocation: String,
    keystorePassword: String,
    keyPassword: String
  )

  case class KafkaConfig(
    bootstrapServers: String,
    topic: String,
    groupId: String,
    startingOffsets: String,
    securityProtocol: String,
    ssl: SslConfig
  )

  case class FieldMapping(
    name: String,
    path: String,
    dataType: String
  )

  case class ApplicationConfig(
    hiveDatabase: String,
    hiveTable: String,
    outputPath: String,
    partitionColumn: String,
    dateFormat: String,
    primaryKeys: List[String],
    dedupLookbackDays: Int,
    customTransformerClass: String,
    fieldMappings: List[FieldMapping]
  ) {
    def fullTableName: String = s"$hiveDatabase.$hiveTable"
    def hasCustomTransformer: Boolean = customTransformerClass.nonEmpty
  }

  case class FilterRule(
    field: String,
    operator: String,
    value: Option[String],
    values: Option[List[String]]
  )

  case class FilterConfig(rules: List[FilterRule])

  case class ErrorConfig(
    errorPath: String,
    enableErrorCapture: Boolean
  )

  // --- Loaders ---

  private def loadSparkConfig(): SparkConfig = {
    val c = rootConfig.getConfig("spark")
    SparkConfig(
      appName            = c.getString("app-name"),
      master             = c.getString("master"),
      enableHive         = c.getBoolean("enable-hive"),
      triggerInterval    = c.getString("trigger-interval"),
      checkpointLocation = c.getString("checkpoint-location")
    )
  }

  private def loadKafkaConfig(): KafkaConfig = {
    val c = rootConfig.getConfig("kafka")
    val s = c.getConfig("ssl")
    KafkaConfig(
      bootstrapServers = c.getString("bootstrap-servers"),
      topic            = c.getString("topic"),
      groupId          = c.getString("group-id"),
      startingOffsets  = c.getString("starting-offsets"),
      securityProtocol = c.getString("security-protocol"),
      ssl = SslConfig(
        truststoreLocation = s.getString("truststore-location"),
        truststorePassword = s.getString("truststore-password"),
        keystoreLocation   = s.getString("keystore-location"),
        keystorePassword   = s.getString("keystore-password"),
        keyPassword        = s.getString("key-password")
      )
    )
  }

  private def loadApplicationConfig(): ApplicationConfig = {
    val c = rootConfig.getConfig("application")
    val fields = c.getConfigList("json.fields").asScala.toList.map { fc =>
      FieldMapping(
        name     = fc.getString("name"),
        path     = fc.getString("path"),
        dataType = fc.getString("type")
      )
    }
    ApplicationConfig(
      hiveDatabase           = c.getString("hive-database"),
      hiveTable              = c.getString("hive-table"),
      outputPath             = c.getString("output-path"),
      partitionColumn        = c.getString("partition-column"),
      dateFormat             = c.getString("date-format"),
      primaryKeys            = c.getStringList("primary-keys").asScala.toList,
      dedupLookbackDays      = c.getInt("dedup-lookback-days"),
      customTransformerClass = c.getString("custom-transformer-class"),
      fieldMappings          = fields
    )
  }

  private def loadFilterConfig(): FilterConfig = {
    val c = rootConfig.getConfig("filter")
    val rules = c.getConfigList("rules").asScala.toList.map { rc =>
      FilterRule(
        field    = rc.getString("field"),
        operator = rc.getString("operator"),
        value    = if (rc.hasPath("value")) Some(rc.getString("value")) else None,
        values   = if (rc.hasPath("values")) Some(rc.getStringList("values").asScala.toList) else None
      )
    }
    FilterConfig(rules)
  }

  private def loadErrorConfig(): ErrorConfig = {
    val c = rootConfig.getConfig("error")
    ErrorConfig(
      errorPath          = c.getString("error-path"),
      enableErrorCapture = c.getBoolean("enable-error-capture")
    )
  }
}
