package uk.co.odinconsultants.shepherd

import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting._
import uk.co.odinconsultants.htesting.spark.SparkForTesting._
import uk.co.odinconsultants.pathologies.Unbalanced._

class SparkSpec extends WordSpec with Matchers {

  "Imbalanced data" should {

    val baseFilename = hdfsUri + System.currentTimeMillis()

    val filenameA = s"${baseFilename}_A"
    val filenameB = s"${baseFilename}_B"
    val ratio     = 0.9f
    val dfA       = write(10L,  session, filenameA, "parquet", ratio)
    val dfB       = write(100L, session, filenameB, "parquet", ratio)

    "aggregatable" in {
      val df          = dfA.groupBy(valueField).agg(count(col(valueField)))
      val class2Count = df.collect().map(r => r.getString(0) -> r.getLong(1)).toMap
      class2Count(LARGE_CLASS) shouldBe 9
      class2Count(SMALL_CLASS) shouldBe 1
    }
    "joinable" in {
      dfA.join(dfB, dfA(idField) <=> dfB(idField)).count() shouldBe 10
    }
  }

}
