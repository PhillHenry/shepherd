package uk.co.odinconsultants.pathologies

import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.pathologies.Unbalanced.writeThenRead

object TwoUnbalancedClasses {

  val baseFilename = hdfsUri + System.currentTimeMillis()

  val filenameA     = s"${baseFilename}_A"
  val filenameB     = s"${baseFilename}_B"
  val ratio         = 0.9f
  val expectedNumA  = 10L
  val expectedNumB  = 100L
  val df            = writeThenRead(expectedNumA,  session, filenameA, "parquet", ratio)
  val expectedLarge = (expectedNumA * ratio).toLong
  val expectedSmall = (expectedNumA * (1f - ratio)).toLong

}
