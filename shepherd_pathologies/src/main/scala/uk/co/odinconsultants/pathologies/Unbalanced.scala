package uk.co.odinconsultants.pathologies

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Unbalanced {

  val idField = "id"
  case class Datum(id: Long, value: String)

  def write(n: Long, session: SparkSession, filename: String, format: String, ratio: Float): DataFrame = {
    import session.implicits._
    val limit = (n * ratio).toLong
    def classification(i: Long): String = if (i < limit) "A" else "B"
    val df = session.range(n).map(i => Datum(i, classification(i)))
    df.write.format(format).save(filename)
    session.read.format(format).load(filename)
  }

}
