package uk.co.odinconsultants.shepherd.diagnostics.partitions

import org.apache.spark.sql.{Dataset, Encoder}

object Skew {

  def count[T: Encoder](xs: Iterator[T]): Long = xs.size

  def partitionSizes[T: Encoder](df: Dataset[T]): Dataset[Long] = {
    import df.sparkSession.implicits._
    df.mapPartitions(xs => Set(count(xs)).toIterator)
  }

  def partitionPopulations[T: Encoder](df: Dataset[T]): Array[Long] =
    partitionSizes(df).collect()

}
