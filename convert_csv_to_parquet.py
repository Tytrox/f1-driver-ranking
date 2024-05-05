import os

from pyspark.sql import SparkSession
from pathlib import Path
from sys import argv


def csv_to_parquet(file: str, output_directory: str) -> None:
    file_name = os.path.basename(file)
    if ".csv" not in file_name:
        raise ValueError(f"Only .csv files are accepted.")
    stripped_extension = os.path.splitext(file_name)[0]
    output_file = os.path.join(output_directory, f"{stripped_extension}.parquet")

    if os.path.exists(output_file):
        print(f"File `{output_file}` already exists, skipping.")
        return

    spark = SparkSession.builder.getOrCreate()

    csv_data = spark.read.csv(file, header=True)
    csv_data.write.parquet(
        output_file,
        compression="zstd", mode="overwrite")


def find_all_csv_files(directory: str) -> list[str]:
    file_list = []
    for csv in Path(directory).rglob("*.csv"):
        file_list.append(str(csv))

    return file_list


def data_directory():
    return "./data"


def usage():
    print(f"Usage: ./{argv[0]} <directory_to_search>")


if __name__ == "__main__":
    data_directory = data_directory()

    if len(argv) > 1:
        search_directory = str(argv[1])
    else:
        print("Directory to search not found.")
        usage()
        exit(1)

    if os.path.isdir(data_directory):
        for path in find_all_csv_files(search_directory):
            csv_to_parquet(path, output_directory=data_directory)
    else:
        print(f"The directory `{data_directory}` does not exist - please "
              f"create it and try again.")
