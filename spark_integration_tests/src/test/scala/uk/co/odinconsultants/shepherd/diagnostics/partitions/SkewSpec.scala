package uk.co.odinconsultants.shepherd.diagnostics.partitions

import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.pathologies.TwoUnbalancedClasses
import uk.co.odinconsultants.pathologies.Unbalanced._
import uk.co.odinconsultants.shepherd.diagnostics.partitions.Skew.partitionPopulations

class SkewSpec extends WordSpec with Matchers {

  import TwoUnbalancedClasses._

  "partitioning on imbalanced column" should {
    "skew the partition populations" in {
      val repartitioned = dfFromDisk.repartition(2, col(valueField))
      val populations   = partitionPopulations(repartitioned)
      populations should have size 2
      populations should contain (expectedLarge)
      populations should contain (expectedSmall)
    }
  }

}
