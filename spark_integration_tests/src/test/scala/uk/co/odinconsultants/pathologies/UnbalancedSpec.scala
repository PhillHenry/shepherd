package uk.co.odinconsultants.pathologies

import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.pathologies.Unbalanced._

class UnbalancedSpec extends WordSpec with Matchers {

  import UnbalancedData._

  "Imbalanced data" should {
    "belong to different classes" in {
      val df            = dfA.groupBy(valueField).agg(count(col(valueField)))
      val class2Count   = df.collect().map(r => r.getString(0) -> r.getLong(1)).toMap
      val expectedLarge = (expectedNumA * ratio).toLong
      val expectedSmall = (expectedNumA * (1f - ratio)).toLong
      class2Count(LARGE_CLASS) shouldBe expectedLarge
      class2Count(SMALL_CLASS) shouldBe expectedSmall
    }
  }

}
