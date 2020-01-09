package uk.co.odinconsultants.pathologies

import org.apache.spark.sql.SparkSession

object Unbalanced {

  case class Datum(id: Long, value: String)

  def write(n: Long, session: SparkSession, filename: String, format: String): Unit = {
    import session.implicits._
    val df = session.range(n).map(i => Datum(i, i.toString))
    df.write.format(format).save(filename)
  }

}
