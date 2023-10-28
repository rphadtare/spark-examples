import org.apache.spark.sql.SparkSession

object SparkDemo {
  def main(args :Array[String]) : Unit = {

    //spark web app ui -> localhost:4040
    val spark = SparkSession.builder().master("local[*]").appName("SparkDemo").
      getOrCreate()

    val l = 1 to 100 by 2

    val rdd = spark.sparkContext.parallelize(l)
    rdd.collect().foreach(n => println(n))

    //Thread.sleep(100000)

    spark.close()
  }

}
