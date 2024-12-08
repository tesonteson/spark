package main.scala

import org.apache.spark.sql.SparkSession


object SparkSQL {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("spark slq usages")
            .master("local[*]")
            .getOrCreate()

        val data = spark.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("./src/main/scala/data/departuredelays.csv")

        data.createOrReplaceTempView("us_delay_flights_tbl")

        val sqlData = spark.sql("select * from us_delay_flights_tbl")

        sqlData.show(5, false)
    }
}