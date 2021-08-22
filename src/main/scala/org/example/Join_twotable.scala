package org.example

import jdk.internal.net.http.frame.DataFrame
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Join_twotable {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc1: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Jointable")
      .getOrCreate()
    sc1.sparkContext.setLogLevel("ERROR")

    val lines = sc1.read.csv("src/main/resources/user.csv")
    val lines1 = sc1.read.csv("src/main/resources/transactions.csv")
    val userColumns = Seq("userid", "mailid", "language", "country")

    val usertable = lines.toDF(userColumns: _*)
    val deptColumns = Seq("transcationid", "productid", "userid", "price", "productdesc")
    val transtable = lines1.toDF(deptColumns: _*)

    println("Inner join")
    usertable.join(transtable, usertable("userid") === transtable("userid"), "inner")
      .show(true)
    println("Outer join")
    usertable.join(transtable, usertable("userid") === transtable("userid"), "outer")
      .show(true)
    println("Left join")
    usertable.join(transtable, usertable("userid") === transtable("userid"), "left")
      .show(true)
    println("Right join")
    usertable.join(transtable, usertable("userid") === transtable("userid"), "right")
      .show(true)
    println("Full join")
    usertable.join(transtable, usertable("userid") === transtable("userid"), "full")
      .show(true)
  }
}