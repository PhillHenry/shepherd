package uk.co.odinconsultants.pathologies

import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.pathologies.Unbalanced.write

object UnbalancedData {

  val baseFilename = hdfsUri + System.currentTimeMillis()

  val filenameA     = s"${baseFilename}_A"
  val filenameB     = s"${baseFilename}_B"
  val ratio         = 0.9f
  val expectedNumA  = 10L
  val dfA           = write(expectedNumA,  session, filenameA, "parquet", ratio)
  val expectedNumB  = 100L
  val dfB           = write(expectedNumB, session, filenameB, "parquet", ratio)

}
