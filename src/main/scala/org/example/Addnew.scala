package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Addnew {
    def main(args: Array[String]): Unit ={

      Logger.getLogger("org").setLevel(Level.ERROR)
         val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  val lines = spark.sparkContext.parallelize(
    Seq("A typical example of RDD-centric functional programming is the following Scala program that computes the frequencies of all words occurring in a set of text files",
      "and prints the most common ones" , "Each map, flatMap (a variant of map) and reduceByKey takes an anonymous function that performs a simple operation on a single data item (or a pair of items)",
      "and applies its argument to transform an RDD into a new RDD."))

  val counts = lines
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
      counts.foreach(println)
  /*
  This also works
  val exam = lines
    .flatMap(line => line.split(" "))
      println(exam.countByValue())
*/
      //sorted result
      val countsorted = counts.map(x=>(x._2,x._1)).sortByKey()
      for(result<- countsorted){
        val c=result._1
        val r= result._2
        println(s"$r,$c")
      }


    }
}