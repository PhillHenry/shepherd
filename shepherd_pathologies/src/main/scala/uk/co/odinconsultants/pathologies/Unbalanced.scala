package uk.co.odinconsultants.pathologies

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Datum(id: Long, value: String)

object Unbalanced extends Serializable {

  val LARGE_CLASS = "A"
  val SMALL_CLASS = "B"

  val idField     = "id"
  val valueField  = "value"

  def classBySplit(limit: Long)(i: Long): String = if (i < limit) LARGE_CLASS else SMALL_CLASS

  def write(n: Long, session: SparkSession, filename: String, format: String, ratio: Float): DataFrame = {
    import session.implicits._
    val limit             = (n * ratio).toLong
    val classificationFn  = classBySplit(limit) _
    val df                = session.range(n).map(i => Datum(i, classificationFn(i)))
    df.write.format(format).save(filename)
    session.read.format(format).load(filename)
  }

}
