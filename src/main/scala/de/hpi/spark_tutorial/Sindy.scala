package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession


object Sindy {
  
  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    println("Discovering INDs in ".concat(inputs.mkString(", ")))

    import spark.implicits._

    val tables = inputs.map(path => spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path)
    )

    val columns = tables.flatMap(df => df.columns.map(col => df.select(col).distinct())) // TODO: not sure atm if unique values give us a benefit

    val valueColumnPairs = columns.map(col => col
      .map(row => (row.get(0).toString, row.schema.fieldNames.head)))
      .reduce((c1, c2) => c1.union(c2)).rdd

    val columnsByValue = valueColumnPairs.groupByKey()

    val attributeSets = columnsByValue.map(_._2.toSet).distinct() // TODO: not sure atm if unique values give us a benefit

    val inclusionLists = attributeSets.flatMap(set => set.map(col => (col, set.filter(otherCol => !otherCol.equals(col)))))

    val aggregates = inclusionLists.reduceByKey((refs1, refs2) => refs1.intersect(refs2)).filter(_._2.nonEmpty)

    val output = aggregates.collect().map(ind => ind._1.concat(" < ").concat(ind._2.mkString(", "))).sorted
    output.foreach(e => println(e))

  }
}
