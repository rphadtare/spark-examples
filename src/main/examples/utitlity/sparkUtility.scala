package utitlity

import org.apache.spark.sql.SparkSession

object sparkUtility {

  def getSparkSession(appName:String = "SparkDemo") : SparkSession = {
    SparkSession.builder().master("local[*]").appName(appName).getOrCreate()
  }

}
