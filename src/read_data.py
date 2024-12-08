import argparse
from pyspark.sql import SparkSession


def main(data_path: str) -> None:
    spark = SparkSession.builder.appName("read data with spark").master("local[*]").getOrCreate()

    data_path = "./data/mnm_dataset.csv"
    data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data_path)
    data.show(5, False)

    spark.stop()

    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser("read data source path")
    parser.add_argument("--data_path", required=True, help="path to data sources like csv files")
    args = parser.parse_args()

    data_path = args.data_path

    main(data_path)
