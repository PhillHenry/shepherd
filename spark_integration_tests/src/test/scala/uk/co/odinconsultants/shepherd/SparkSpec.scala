package uk.co.odinconsultants.shepherd

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting._
import uk.co.odinconsultants.htesting.spark.SparkForTesting._
import uk.co.odinconsultants.pathologies.Unbalanced._

class SparkSpec extends WordSpec with Matchers {

  import session.implicits._

  "Spark" should {

    val baseFilename = hdfsUri + System.currentTimeMillis()

    val filenameA = s"${baseFilename}_A"
    val filenameB = s"${baseFilename}_B"
    val dfA = write(10L, session, filenameA, "parquet")
    val dfB = write(100L, session, filenameB, "parquet")

    "be up and running" in {
      dfA.join(dfB, dfA(idField) === dfB(idField)).count() shouldBe 10
    }
  }

}
