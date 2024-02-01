import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object SparkDemo {
  def main(args :Array[String]) : Unit = {

    //spark web app ui -> localhost:4040
    val spark = SparkSession.builder().master("local[*]").appName("SparkDemo").
      getOrCreate()

    //val l = 1 to 100 by 2

    //val rdd = spark.sparkContext.parallelize(l)
    //rdd.collect().foreach(n => println(n))

    //Thread.sleep(100000)

    import spark.implicits._
    List(("A",100),("B",102),("C",101),("D",104),("E",103),("F",101),("F",100)).toDF("id","sal")
      .createOrReplaceTempView("employee")


    /*spark.sql("select id,sal from employee e1 " +
      "where not exists" +
      "(select 1 from employee e2 where e2.sal > e1.sal)").show(false)

    val df = spark.sql("select id,sal from employee e1 " +
      "where 2 = " +
      "(select count(distinct sal) from employee e2 where e2.sal >= e1.sal)")
    */

    val df = spark.sql("select id,count(1) as cnt from employee group by 1")
    df.printSchema()


    spark.close()
  }

}
