package uk.co.odinconsultants.shepherd.diagnostics.plans

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.command.ExplainCommand

object PlanParser {

  def diagnose(df: DataFrame, session: SparkSession): Unit = {
    df.explain()
    val explain = ExplainCommand(df.queryExecution.logical, extended = true)
    session.sessionState.executePlan(explain).executedPlan.executeCollect().foreach {
      r => println(r.getString(0))
    }
    ???
  }

}
