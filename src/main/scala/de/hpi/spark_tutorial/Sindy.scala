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


//    val cells = tables.flatMap(df => df.columns.map(col => df.select(col))
    val columns = tables.flatMap(df => df.columns.map(col => df.select(col).distinct())) // not sure atm if unique values give us a benefit
    //columns.foreach(df => df.show())


    val cells = columns.map(col => {
//      val colName = col.schema.fieldNames.head
//      col.map(row => row)
    })

    columns.foreach(column => {
      println(column.schema.fieldNames.head)
      column.foreach(row => {
        val value = row.get(0)
//        val colName = row.schema.fieldNames

      })
      println("----")
    })


/*
    val columnsPerTable = tables.map(df => df.columns.map(colname => df.select(colname)))
    //columnsPerTable.foreach(table => table.foreach(col => col.show()))

    val distinctColumnsPerTable = columnsPerTable.map(table => table.map(c => c.distinct()))
    //distinctColumnsPerTable.foreach(table => table.foreach(col => col.show()))

    val distinctColumns = distinctColumnsPerTable.flatMap(arrayOfCols => arrayOfCols)
    //distinctColumns.foreach(col => col.show())
    */

  }
}
