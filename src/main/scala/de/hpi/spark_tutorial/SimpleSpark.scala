package de.hpi.spark_tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.commons.cli._


object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {
    //config values
    var cores = 4
    var path = "./TPCH"

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // CLI parsing
    val options = new Options
    val pathOpt = Option.builder().longOpt("path").hasArg().desc("path to the TPCH files").build()
    val coresOpt = Option.builder().longOpt("cores").hasArg().desc("number of worker cores").build()
    options.addOption(pathOpt)
    options.addOption(coresOpt)

    val parser = new DefaultParser
    try {
      val cmd = parser.parse(options, args)
      path =  cmd.getOptionValue("path", "./TPCH")
      cores = cmd.getOptionValue("cores", "4").toInt
    } catch {
      case e : Exception => {
        println(e, e.getMessage)
        return
      }
    }


    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master(s"local[$cores]") // local, with $cores worker cores
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
