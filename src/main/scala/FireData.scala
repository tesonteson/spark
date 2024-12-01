package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object FireData {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("fire data processing")
            .master("local[*]")
            .getOrCreate

        val dataPath = args(0)

        val schema = StructType(Array(
            StructField("CallNumber", IntegerType, true),
            StructField("UnitID", StringType, true),
            StructField("IncidentNumber", IntegerType, true),
            StructField("CallType", StringType, true),
            StructField("CallDate", StringType, true),
            StructField("WatchDate", StringType, true),
            StructField("CallFinalDisposition", StringType, true),
            StructField("AvailableDtTm", StringType, true),
            StructField("Address", StringType, true),
            StructField("City", StringType, true),
            StructField("Zipcode", IntegerType, true),
            StructField("Battalion", StringType, true),
            StructField("StationArea", StringType, true),
            StructField("Box", StringType, true),
            StructField("OriginalPriority", StringType, true),
            StructField("Priority", IntegerType, true),
            StructField("FinalPriority", IntegerType, true),
            StructField("ALSUnit", BooleanType, true),
            StructField("CallTypeGroup", StringType, true),
            StructField("NumAlarms", IntegerType, true),
            StructField("UnitType", StringType, true),
            StructField("UnitSequenceInCallDispatch", IntegerType, true),
            StructField("FirePreventionDistrict", StringType, true),
            StructField("SupervisorDistrict", StringType, true),
            StructField("Neighborhood", StringType, true),
            StructField("Location", StringType, true),
            StructField("RowID", StringType, true),
            StructField("Delay", FloatType, true)
        ))

        val data = spark.read.schema(schema).format("csv").load(dataPath)
        val processedData = data
            .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
            .drop("CallDate")
            .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
            .drop("WatchDate")
            .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
            .drop("AvailableDtTm")

        val editedData = processedData
            .select("IncidentNumber", "AvailableDtTS", "CallType")
            .where(col("CallType") =!= "Medical Incident")

        val distinctData = processedData
            .select("CallType")
            .where(col("CallType").isNotNull)
            .distinct()

        val dateData = processedData
            .select(year(col("IncidentDate")))
            .distinct()
            .orderBy(year(col("IncidentDate")))

        val mostCallType = processedData
            .select("CallType")
            .where(col("CallType").isNotNull)
            .groupBy("CallType")
            .count()
            .orderBy(desc("count"))

        editedData.show(5, false)
        distinctData.show(5, false)
        dateData.show(5, false)
        mostCallType.show(5, false)

        spark.stop()

    }
}