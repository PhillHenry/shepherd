package uk.co.odinconsultants.shepherd.diagnostics.partitions

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.pathologies.TwoUnbalancedClasses._
import uk.co.odinconsultants.pathologies.Unbalanced.LARGE_CLASS

class FilesSpec extends WordSpec with Matchers {

  import Files._

  "A dataset saved on HDFS" should {
    "have stats associed with it" in {
      val fileStats = filesStatsOf(dfFromDisk)
      fileStats should not be empty
      println(fileStats.mkString("\n"))

      fileStats.values.map(_.minAsString()).min shouldBe LARGE_CLASS
    }
  }

}
