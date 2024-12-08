package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object MnMcount {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("MnMcount")
            .master("local[*]")
            .getOrCreate()

        if (args.length < 2) {
          println("Usage: MnMcount <mnm_file_dataset> <output_data_path")
          sys.exit(1)
        }

        val mnmFile = args(0)
        val outputPath = args(1)

        val schema = StructType(Array(
            StructField("State", StringType, false),
            StructField("Color", StringType, false),
            StructField("Count", IntegerType, false),
            ))
        val mnmDF = spark.read
            .schema(schema)
            .format("csv")
            .option("header", "true")
            .load(mnmFile)
        val caCountMnNDF = mnmDF
            .select("State", "Color", "Count")
            .where(col("State") === "CA")
            .groupBy("State", "Color")
            .sum("Count")
            .withColumnRenamed("sum(Count)", "TotalCount")
            .orderBy(desc("TotalCount"))
        val caCountMnNDF_double = caCountMnNDF.withColumn("DoubleTotalCount", col("TotalCount") * 2)

        caCountMnNDF.show(10)
        caCountMnNDF_double.show(10)

        caCountMnNDF.write.mode("overwrite").format("csv").save(outputPath)

        spark.stop()
    }
}
