import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main(input_path, output_path):
    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("count mnm data with spark")
             .getOrCreate())

    schema = StructType([
        StructField("State", StringType(), False),
        StructField("Color", StringType(), False),
        StructField("Count", IntegerType(), False)
    ])
    mnm_df = (spark.read
              .schema(schema)
              .format("csv")
              .option("header", "true")
              .load(input_path))
    agg_mnm_df = (mnm_df
                  .select("State", "Color", "Count")
                  .where(col("State") == "CA")
                  .groupBy("State", "Color")
                  .sum("Count")
                  .withColumnRenamed("sum(Count)", "TotalCount")
                  .orderBy("TotalCount", ascending=True))
    agg_mnm_df.show(5, False)

    agg_mnm_df.write.mode("overwrite").format("csv").save(output_path)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()

    input_path =args.input_path
    output_path = args.output_path

    main(input_path, output_path)
