import os.path
from functools import lru_cache
from pyspark.sql import SparkSession, DataFrame
from os import path
from convert_csv_to_parquet import data_directory

spark = None


@lru_cache
def get_df_from_file(filename: str) -> DataFrame:
    global spark
    filepath = path.join(data_directory(), f"{filename}.parquet")

    if not os.path.exists(filepath):
        raise ValueError(
            "The provided filename does not correspond to a valid path")

    if spark is None:
        spark = (
            SparkSession.builder
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

    return spark.read.parquet(filepath)
