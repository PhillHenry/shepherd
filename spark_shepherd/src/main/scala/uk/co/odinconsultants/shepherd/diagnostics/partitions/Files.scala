package uk.co.odinconsultants.shepherd.diagnostics.partitions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

import scala.collection.JavaConverters._

object Files {

  def filesStatsOf[U <: Comparable[U]](ds: DataFrame): Map[String, Statistics[T] forSome {type T <: Comparable[T]}] = {
    val conf = new Configuration()
    conf.setBoolean("parquet.strings.signed-min-max.enabled", true)
    ds.inputFiles.flatMap { file =>
      val reader = ParquetFileReader.readFooter(conf, new Path(file), ParquetMetadataConverter.NO_FILTER)
      for {
        block   <- reader.getBlocks.asScala
        column  <- block.getColumns.asScala
      } yield {
        file -> column.getStatistics
      }
    }.toMap
  }

}
