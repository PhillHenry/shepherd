package uk.co.odinconsultants.shepherd

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting._
import uk.co.odinconsultants.htesting.spark.SparkForTesting._
import uk.co.odinconsultants.pathologies.Unbalanced
import uk.co.odinconsultants.pathologies.Unbalanced.write

class SparkSpec extends WordSpec with Matchers {

  import session.implicits._

  "Spark" should {

    val baseFilename = hdfsUri + System.currentTimeMillis()

    val filenameA = s"${baseFilename}_A"
    val filenameB = s"${baseFilename}_B"
    write(10L, session, filenameA, "parquet")
    write(100L, session, filenameB, "parquet")

    "be up and running" in {
      val idField = "id"
      val dfA = session.read.parquet(filenameA)
      val dfB = session.read.parquet(filenameB)

      dfA.join(dfB, dfA(idField) === dfB(idField)).count() shouldBe 10
    }
  }

}
