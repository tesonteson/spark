package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DeviceIoT {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("Device IoT Data Processing")
            .master("local[*]")
            .getOrCreate()

        import spark.implicits._

        val data = spark.read
            .json("./src/main/scala/data/iot_devices.json")
            .as[DeviceIoTData]

        val brokenData = data
            .filter({d => d.battery_level <= 3})
            .map(d => BrokenDeviceData(d.device_id, d.battery_level))
            .as[BrokenDeviceData]

        val co2Data = data
            .groupBy("cn")
            .agg(avg("c02_level").alias("avg_c02_level"))
            .orderBy(desc("avg_c02_level"))
            .as[CO2DeviceData]

        data.show(5, false)
        brokenData.show(5, false)
        co2Data.show(5, false)
    }
}