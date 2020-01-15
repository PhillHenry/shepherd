package uk.co.odinconsultants.shepherd.diagnostics.partitions

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.pathologies.TwoUnbalancedClasses._
import uk.co.odinconsultants.pathologies.Unbalanced._

class FilesSpec extends WordSpec with Matchers {

  import Files._

  "A dataset saved on HDFS" should {
    "have stats associed with it" in {
      val fileStats = filesStatsOf(dfFromDisk)
      fileStats should not be empty
      withClue(fileStats.mkString("\n")) {
        val valueStats = fileStats.filter { case ((_, column), _) => column == valueField }
        valueStats.values.map(_.minAsString).min shouldBe LARGE_CLASS
      }
    }
  }

}
