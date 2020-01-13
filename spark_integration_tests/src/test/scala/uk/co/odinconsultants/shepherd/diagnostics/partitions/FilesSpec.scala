package uk.co.odinconsultants.shepherd.diagnostics.partitions

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.pathologies.TwoUnbalancedClasses._

class FilesSpec extends WordSpec with Matchers {

  import Files._

  "A dataset saved on HDFS" should {
    "have stats associed with it" in {
      val fileStats = filesStatsOf(dfFromDisk)
      fileStats should not be empty
      println(fileStats.mkString("\n"))
    }
  }

}
