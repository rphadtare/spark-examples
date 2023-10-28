object HelloScala {

  def main(args : Array[String]): Unit = {

    println("This is scala!!!")

    val s1 = "Ht:172,Wt:76|Ht:166,Wt:51|Ht:156,Wt:48|Ht:172,Wt:88|Ht:158,Wt:48"
    s1.split("\\|").foreach(println)

    Seq((1,31),(1,23),(1,12)).foreach(
        a => {
        println(a._1)
      }
    )


  }

}
