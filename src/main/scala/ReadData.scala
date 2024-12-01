package main.scala

import org.apache.spark.sql.SparkSession


object ReadData {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("read data folder")
            .master("local[*]")
            .getOrCreate()

        val data = spark.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("./src/main/scala/data/mnm_dataset_output")

        data.show(5, false)

        spark.stop()
    }
}