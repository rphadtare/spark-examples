import org.apache.spark.sql.SparkSession
object SparkDemo {
  def main(args :Array[String]) : Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("SparkDemo").getOrCreate()
    spark.sparkContext.setLogLevel("error")

    val rdd = spark.sparkContext.parallelize(List(1, 2, 3))
    rdd.collect().foreach(n => println(n))

    spark.close()

  }

}
