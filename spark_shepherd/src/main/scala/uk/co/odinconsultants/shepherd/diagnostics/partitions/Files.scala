package uk.co.odinconsultants.shepherd.diagnostics.partitions

import org.apache.spark.sql.{DataFrame, Row}

case class Statistics(count: Long, mean: Option[Double], stddev: Option[Double], minAsString: String, maxAsString: String)

object Files {

  def filesStatsOf[U <: Comparable[U]](ds: DataFrame): Map[(String, String), Statistics] = {
    val session = ds.sparkSession
    val columns = ds.columns
    val fileCols2Stats = for {
      file    <- ds.inputFiles
      column  <- columns
    } yield {
      val subDF = session.read.load(file)

      (file, column) -> toStats(subDF.describe(column).collect())
    }
    fileCols2Stats.toMap
  }

  def toOptionalDouble(x: String): Option[Double] = Option(x).map(_.toDouble)

  def toStats(rs: Array[Row]): Statistics = {
    val name2Stat = rs.map { r =>
      r.getString(0) -> r.getString(1)
    }.toMap
    Statistics(name2Stat("count").toLong, toOptionalDouble(name2Stat("mean")), toOptionalDouble(name2Stat("stddev")), name2Stat("min"), name2Stat("max"))
  }

}
