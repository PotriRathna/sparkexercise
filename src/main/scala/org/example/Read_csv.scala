package org.example

import org.apache.spark._
import org.apache.log4j._


object Read_csv {

  def parseLine(line: String): (Int, Int) = {
    // Split by commas
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("src/main/resources/friends.csv")

    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
     totalsByAge.foreach(println)
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
    val results = averagesByAge.collect()
    results.sorted.foreach(println)
  }
}
