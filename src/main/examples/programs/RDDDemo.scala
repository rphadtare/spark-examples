package programs

import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import utitlity.sparkUtility
import org.apache.spark.sql._

import scala.reflect.classTag

object RDDDemo{

  case class Body(height:Int, weight:Int)

  var flag = 0

  //Compare method needed to compare two Body objects
  implicit val bodyObjOrdering: Ordering[Body] = (x: Body, y: Body) => {
    if (x.height > y.height) {
      1
    } else if (x.height < y.height) {
      -1
    } else if (x.weight < y.weight) {
      1
    } else if (x.weight > y.weight) {
      -1
    } else {
      0
    }
  }

  def main(args : Array[String]): Unit = {

    val spark = sparkUtility.getSparkSession("RDD Demo")
    import spark.implicits._

    //create RDD from scala structures
    val listRdd = spark.sparkContext.parallelize(1 to 100 by 4)

    //traverse through rdd and print all elements
    if(flag == 1) {
      //listRdd.collect().foreach(println)
      println(s"Count of listRdd : ${listRdd.count()}")
    }


    //create rdd from text
    val str = List("Ht:172,Wt:76|Ht:166,Wt:51|Ht:156,Wt:48|Ht:172,Wt:88|Ht:158,Wt:48",
              "Ht:172,Wt:76|Ht:166,Wt:59|Ht:157,Wt:49|Ht:173,Wt:88|Ht:158,Wt:48"
              )
    val inputStrRdd = spark.sparkContext.parallelize(str)

    //print str rdd
    if(flag == 1) {
      println("Printing str rdd : ")
      inputStrRdd.collect().foreach(println)
    }

    // transform rdd into rdd[Body]
    val bodyRdd = inputStrRdd.flatMap(s => s.split("\\|"))
      .map(
      rec =>
        {
          val height = rec.split(",")(0).split(":")(1).toInt
          val weight = rec.split(",")(1).split(":")(1).toInt
          Body(height, weight)
        }
    )

    if(flag == 1) {
      println("Printing body rdd : ")
      bodyRdd.collect().foreach(println)
    }

    //body pair rdd
    val bodyPairRdd = bodyRdd.map(r => (r,1)).reduceByKey(
      func = (a, b) => a+b
    )

    //get only those Body types which are identical in height and weight
    if (flag == 1) {
      println("Printing identical body elements : ")
      bodyPairRdd.filter(r => r._2 > 1).sortByKey(ascending = true).foreach(println)
    }

    //to print body rdd according to height desc, weight asc
    if (flag == 1) {
      println("Printing sorted body elements : ")
      bodyRdd.sortBy(x => x,ascending = false,1)(bodyObjOrdering,classTag[Body]).collect().foreach(println)
    }

    //group by
    if (flag == 1) {
      println("Group by example")
      listRdd.map(x => if(x%5 == 0){
        (1,x)
        }else{
        (0,x)
        }
      ).groupBy(x => x._1)
        .map(
          rec => {
            var subtotal = 0
            rec._2.foreach(
              x => {
                subtotal = subtotal + x._2
              }
            )
            (rec._1,subtotal)
          }
        ).collect().foreach(println)

    }

    //Aggregate by key example
    if(flag == 1){
      println("Aggregate By key example")
      val list2 = List(("A", 100), ("B", 200), ("A", 80), ("B", 20),("C",25))
      val rdd = spark.sparkContext.parallelize(list2)

      rdd.aggregateByKey(0)(
        (subtotal,num) => {
          subtotal + num
        },
        (acc1,acc2) => {
          acc1 + acc2
        }
      ).collect().foreach(println)

    }

    //Count by key example
    if (flag == 1) {
      println("Count By key example")
      val list2 = List(("A", 100), ("B", 200), ("A", 80), ("B", 20), ("C", 25))
      val rdd = spark.sparkContext.parallelize(list2)
      rdd.countByKey().foreach(println)
    }

    //Group by key example
    if (flag == 1) {
      println("Group By key example")
      val list2 = List(("A", 200), ("B", 200), ("A", 200), ("B", 20), ("C", 25))
      val rdd = spark.sparkContext.parallelize(list2)

      rdd.groupByKey().map(r => (r._1, r._2.sum)).collect.foreach(println)

      rdd.groupByKey().map(r => (r._1, r._2.sum)).repartition(1)
        .saveAsTextFile("src/main/resources/group_by_key_op/abc.txt")

    }

    //join two rdd's
    if(flag != 1){

      import spark.implicits._

      val empList = Seq((10,"Rohit"),(20,"Pooja"),(10,"Rajani"))
      val deptList = Seq((10,"IT"),(20,"Civil"),(30,"Automotive"))

      val empRdd = spark.sparkContext.parallelize(empList)
      val deptRdd = spark.sparkContext.parallelize(deptList)

      deptRdd.leftOuterJoin(empRdd).map(r => (r._1, r._2._1, r._2._2.getOrElse(""))).collect().foreach(println)

    }



    spark.close()

  }

}
