package programs

import utitlity.sparkUtility
import org.apache.spark.sql._

object DataFrameDemo {

  val flag = 1
  def main(args : Array[String]) : Unit = {
    val spark = sparkUtility.getSparkSession("DataFrame Demo")
    import spark.implicits._

    if(flag == 1) {

      val arr = Array("country", "state", "population")
      val str = arr.map(r => (r, arr.indexOf(r) + 1)).
        map(r => "_" + r._2 + " AS " + r._1).mkString(",")

      List(("INDIA","MH",200),("INDIA","RJ",150),("SWISS","ZH",100),("SWISS","LS",300))
        .toDF().createTempView("sample")

      val query = s"select $str from sample"
      println(query)

      spark.sql(s"$query").show(false)


    }


    spark.close()
  }



}
