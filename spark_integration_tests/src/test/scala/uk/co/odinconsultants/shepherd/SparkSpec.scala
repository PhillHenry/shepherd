package uk.co.odinconsultants.shepherd

import org.scalatest.{Matchers, WordSpec}

import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting._
import uk.co.odinconsultants.htesting.spark.SparkForTesting._

class SparkSpec extends WordSpec with Matchers {

  import session.implicits._

  "Spark" should {

    val a = (1 to 10).map(i => i -> i.toString)
    val b = (1 to 100).map(i => i -> i.toString)

    "be up and running" in {
      val idField = "id"
      val dfA = a.toDF(idField, "value")
      val dfB = b.toDF(idField, "value")

      dfA.join(dfB, dfA(idField) === dfB(idField)).count() shouldBe 10
    }
  }

}
