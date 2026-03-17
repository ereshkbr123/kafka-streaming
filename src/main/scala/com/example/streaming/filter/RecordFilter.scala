package com.example.streaming.filter

import com.example.streaming.config.AppConfig
import AppConfig.FilterRule
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
 * Applies filter rules from config. AND logic — all rules must pass.
 * Supported: equals, not_equals, in, not_in, is_not_null, is_null
 */
object RecordFilter {

  def apply(df: DataFrame): DataFrame = {
    val rules = AppConfig.filter.rules

    if (rules.isEmpty) {
      df
    } else {
      val combined = rules.map(buildCondition).reduce(_ && _)
      df.filter(combined)
    }
  }

  private def buildCondition(rule: FilterRule): Column = {
    rule.operator.toLowerCase match {

      case "equals" =>
        col(rule.field) === lit(rule.value.getOrElse(
          throw new IllegalArgumentException(s"'equals' requires 'value' for field: ${rule.field}")
        ))

      case "not_equals" =>
        col(rule.field) =!= lit(rule.value.getOrElse(
          throw new IllegalArgumentException(s"'not_equals' requires 'value' for field: ${rule.field}")
        ))

      case "in" =>
        val vals = rule.values.getOrElse(
          throw new IllegalArgumentException(s"'in' requires 'values' for field: ${rule.field}")
        )
        col(rule.field).isin(vals.map(lit(_)): _*)

      case "not_in" =>
        val vals = rule.values.getOrElse(
          throw new IllegalArgumentException(s"'not_in' requires 'values' for field: ${rule.field}")
        )
        !col(rule.field).isin(vals.map(lit(_)): _*)

      case "is_not_null" =>
        col(rule.field).isNotNull

      case "is_null" =>
        col(rule.field).isNull

      case other =>
        throw new IllegalArgumentException(s"Unsupported filter operator: $other for field: ${rule.field}")
    }
  }
}
