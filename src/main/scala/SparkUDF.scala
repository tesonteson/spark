package main.scala

import org.apache.spark.sql.SparkSession


object SparkUDF {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("spark udf")
            .master("local[*]")
            .getOrCreate()

        val cubed = (s: Long) => {
            s * s * s
        }

        spark.udf.register("cubed", cubed)

        spark.range(1, 9).createOrReplaceTempView("udf_test")

        spark.sql("select id, cubed(id) as id_cubed from udf_test").show()
    }
}
