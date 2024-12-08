package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SparkMysql {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("read mysql with jdbc on spark")
            .master("local[*]")
            .getOrCreate()

        val query =
            """
              |select
              |e.id as employee_id,
              |e.name as emplyee_name,
              |e.department_id,
              |d.department_name
              |from employees as e
              |left outer join departments as d
              |on e.department_id = d.id
              |""".stripMargin

        val data = spark.read
            .format("jdbc")
            .option("url", "jdbc:mysql://db.spark:3306/spark")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("query", query)
            .option("user", "root")
            .option("password", "root")
            .load()

      data
          .groupBy("department_name")
          .agg(count(col("department_name")).alias("num"))
          .orderBy(desc("num"))
          .show()
    }
}
