package main.scala

import org.apache.spark.sql.SparkSession


object HigherOrderFunc {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("higher order function")
            .master("local[*]")
            .getOrCreate()

        import spark.implicits._

        val t1 = Array(35, 36, 32, 30, 40, 42, 38)
        val t2 = Array(31, 32, 34, 55, 56)
        val tc = Seq(t1, t2).toDF("celsius")
        tc.createOrReplaceTempView("tc")

        tc.show(false)

        spark.sql("select celsius, transform(celsius, t -> t + 100) as plus100 from tc").show(false)
        spark.sql("select celsius, filter(celsius, t -> t > 38) as high_th_38 from tc").show(false)
        spark.sql("select celsius, exists(celsius, t -> t = 38) as threshold_with_38 from tc").show(false)
        spark.sql("select celsius, reduce(celsius, 0, (t, acc) -> t + acc, acc -> acc div size(celsius)) as avgFahrenheit from tc").show(false)

        spark.stop()
    }
}