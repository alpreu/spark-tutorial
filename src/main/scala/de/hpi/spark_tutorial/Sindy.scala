package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession, functions}


object Sindy {

  /*
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq("Roses are red", "Violets are blue"))  // lines

    println("data")
    rdd.collect.foreach(s => print(s + " | "))
    println()

    println("map")
    rdd.map(_.split(" ")).collect.foreach(s => print(s.foreach(i => print(i + ",")) + " | "))
    println()

    println("flatmap")
    rdd.flatMap(_.split(" ")).collect.foreach(s => print(s + " | "))
    println()
    */
  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    println(inputs)

    import spark.implicits._

    val tables = inputs.map(path => spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path)
    )

//    tables.foreach(df => df.show())
//    tables.foreach(df => df.printSchema())


//    val columns = tables.flatMap(df => df.columns.map(col => df.select(col)))
    val columns = tables.flatMap(df => df.columns.map(col => df.select(col).distinct())) // not sure atm if unique values give us a benefit
//    columns.foreach(df => df.show())


    val valueColumnPairs = columns.map(col => (
      col.map(row => (row.get(0).toString, row.schema.fieldNames.head))
    )).reduce((c1, c2) => c1.union(c2)).rdd
//    valueColumnPairs.foreach(c => println(c))

    val columnsByValue = valueColumnPairs.groupByKey()//.mapValues(List(_))
    columnsByValue.foreach(e => println(e))


  }
}
