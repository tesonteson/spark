package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SparkWindowFunc {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("spark window function")
            .master("local[*]")
            .getOrCreate()

        val departureDataPath = "./src/main/scala/data/departuredelays.csv"
        val airportDataPath = "./src/main/scala/data/airport-codes-na.txt"

        val delayData = spark.read
            .format("csv")
            .option("header", true)
            .option("inferSchema", true)
            .load(departureDataPath)
        val airportData = spark.read
            .format("csv")
            .option("header", true)
            .option("inferSchema", true)
            .option("delimiter", "\t")
            .load(airportDataPath)
            .withColumn("delay", expr("CAST(delay as INT) as delay"))
            .withColumn("distance"< expr("CAST(distance as INT) as distance"))

        delayData.createOrReplaceTempView("departureDelays")
        airportData.createOrReplaceTempView("airports")


    }
}