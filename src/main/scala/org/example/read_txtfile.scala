package org.example
import org.apache.log4j._
import org.apache.spark._
object read_txtfile {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val input = sc.textFile("src/main/resources/data.txt")

    val words = input.flatMap(x => x.split(" "))
    val lowercase = words.map(_.toLowerCase)

    val wordCounts = lowercase.countByValue()

    wordCounts.foreach(println)
  }

}
