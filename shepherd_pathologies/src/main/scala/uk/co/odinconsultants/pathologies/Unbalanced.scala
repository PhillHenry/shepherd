package uk.co.odinconsultants.pathologies

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Datum(id: Long, value: String)

object Unbalanced extends Serializable {

  val LARGE_CLASS = "A"
  val SMALL_CLASS = "B"

  val idField     = "id"
  val valueField  = "value"

  def classBySplit(limit: Long)(i: Long): String = if (i < limit) LARGE_CLASS else SMALL_CLASS

  def writeThenRead(n: Long, session: SparkSession, filename: String, format: String, ratio: Float): DataFrame = {
    val df = twoCategories(n, session, ratio)
    df.write.format(format).save(filename)
    session.read.format(format).load(filename)
  }

  def twoCategories(n: Long, session: SparkSession, ratio: Float): Dataset[Datum] = {
    import session.implicits._
    val limit             = (n * ratio).toLong
    val classificationFn  = classBySplit(limit) _
    session.range(n).map(i => Datum(i, classificationFn(i)))
  }
}
