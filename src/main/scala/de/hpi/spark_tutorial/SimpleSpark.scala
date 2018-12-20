package de.hpi.spark_tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

case class Config(tpchPath: String = "./TPCH/", cores: Int = 4)

object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val parser = new scopt.OptionParser[Config]("SparkTutorial-1.0") {
      head("SparkTutorial", "1.0")

      opt[String]("path") action { (x, c) => {
          if (!x.endsWith("/")) {
            c.copy(tpchPath = x + "/")
          }
          else {
            c.copy(tpchPath = x)
          }
        }
      } text("Path to the directory with the TPCH files [./TPCH]")

      opt[Int]("cores").action { (x, c) =>
        c.copy(cores = x) } text("Number of cores to use [4]")
    }

    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>

        // Create a SparkSession to work with Spark
        val sparkBuilder = SparkSession
          .builder()
          .appName("SparkTutorial")
          .master(s"local[${config.cores}]")

        val spark = sparkBuilder.getOrCreate()

        // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
        spark.conf.set("spark.sql.shuffle.partitions", s"${config.cores}")

        // Utility method
        def time[R](block: => R): R = {
          val t0 = System.currentTimeMillis()
          val result = block
          val t1 = System.currentTimeMillis()
          println(s"Execution: ${t1 - t0} ms")
          result
        }

        val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
          .map(name => s"${config.tpchPath}tpch_$name.csv")

        time {Sindy.discoverINDs(inputs, spark)}

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }
}
