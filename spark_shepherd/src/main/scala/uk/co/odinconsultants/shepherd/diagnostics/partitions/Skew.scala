package uk.co.odinconsultants.shepherd.diagnostics.partitions

import org.apache.spark.sql.{DataFrame, Dataset}

object Skew {

  def count(xs: Iterator[_]): Long = xs.size

  def partitionSizes(df: DataFrame): Dataset[Long] = {
    import df.sparkSession.implicits._
    df.mapPartitions(xs => Set(count(xs)).toIterator)
  }

  def partitionPopulations(df: DataFrame): Array[Long] =
    partitionSizes(df).collect()

}
