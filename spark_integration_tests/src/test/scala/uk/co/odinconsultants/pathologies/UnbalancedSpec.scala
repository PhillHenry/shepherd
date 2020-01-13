package uk.co.odinconsultants.pathologies

import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.pathologies.Unbalanced._

class UnbalancedSpec extends WordSpec with Matchers {

  import TwoUnbalancedClasses._

  "Imbalanced data" should {
    "belong to different classes" in {
      val aggregated    = df.groupBy(valueField).agg(count(col(valueField)))
      val class2Count   = aggregated.collect().map(r => r.getString(0) -> r.getLong(1)).toMap
      class2Count(LARGE_CLASS) shouldBe expectedLarge
      class2Count(SMALL_CLASS) shouldBe expectedSmall
    }
  }

}
