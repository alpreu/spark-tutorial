package de.hpi.spark_tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    // Utility method
    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"data/TPCH/tpch_$name.csv")

    time {Sindy.discoverINDs(inputs, spark)}
  }
}
