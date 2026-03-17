package com.yourcompany.streaming.config

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
    customFlattenerClass: String,
    fieldMappings: List[FieldMapping],
    extraParams: Map[String, String]       // open bag — any extra key-value from config
  ) {
    def fullTableName: String = s"$hiveDatabase.$hiveTable"
    def hasCustomTransformer: Boolean = customTransformerClass.nonEmpty
    def hasCustomFlattener: Boolean   = customFlattenerClass.nonEmpty
    def param(key: String): String    = extraParams.getOrElse(key, "")
    def param(key: String, default: String): String = extraParams.getOrElse(key, default)
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
    KafkaConfig(
      bootstrapServers = c.getString("bootstrap-servers"),
      topic            = c.getString("topic"),
      groupId          = c.getString("group-id"),
      startingOffsets  = c.getString("starting-offsets"),
      securityProtocol = c.getString("security-protocol"),
      ssl = SslConfig(
        truststoreLocation = sys.props.getOrElse("ssl.truststore.location", ""),
        truststorePassword = sys.props.getOrElse("ssl.truststore.password", ""),
        keystoreLocation   = sys.props.getOrElse("ssl.keystore.location",   ""),
        keystorePassword   = sys.props.getOrElse("ssl.keystore.password",   ""),
        keyPassword        = sys.props.getOrElse("ssl.key.password",        "")
      )
    )
  }

  private def loadApplicationConfig(): ApplicationConfig = {
    val c = rootConfig.getConfig("application")

    val typeOverrides: Map[String, String] =
      if (c.hasPath("json.type-overrides"))
        c.getConfigList("json.type-overrides").asScala.toList
          .map(tc => tc.getString("name") -> tc.getString("type"))
          .toMap
      else Map.empty

    val fields = c.getConfigList("json.groups").asScala.toList.flatMap { gc =>
      val pathPrefix = gc.getString("path")
      gc.getString("columns").split(",").map(_.trim).filter(_.nonEmpty).map { col =>
        val parts    = col.split(":").map(_.trim)
        val srcName  = parts(0)
        val tgtName  = if (parts.length > 1) parts(1) else srcName
        val fullPath = if (pathPrefix.isEmpty) srcName else s"$pathPrefix.$srcName"
        FieldMapping(
          name     = tgtName,
          path     = fullPath,
          dataType = typeOverrides.getOrElse(tgtName, "string")
        )
      }
    }

    // Parse extra-params block into a flat Map — open bag for any user-defined keys
    val extraParams: Map[String, String] =
      if (c.hasPath("extra-params")) {
        c.getConfig("extra-params").entrySet().asScala
          .map(e => e.getKey -> e.getValue.unwrapped().toString)
          .toMap
      } else Map.empty

    ApplicationConfig(
      hiveDatabase           = c.getString("hive-database"),
      hiveTable              = c.getString("hive-table"),
      outputPath             = c.getString("output-path"),
      partitionColumn        = c.getString("partition-column"),
      dateFormat             = c.getString("date-format"),
      primaryKeys            = c.getStringList("primary-keys").asScala.toList,
      dedupLookbackDays      = c.getInt("dedup-lookback-days"),
      customTransformerClass = c.getString("custom-transformer-class"),
      customFlattenerClass   = if (c.hasPath("custom-flatten-class")) c.getString("custom-flatten-class") else "",
      fieldMappings          = fields,
      extraParams            = extraParams
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
