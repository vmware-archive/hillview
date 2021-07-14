#!/usr/bin/env python3
import os
import os.path as path
import shutil

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

base_path = path.join(path.dirname(path.dirname(path.abspath(__file__))), "data")
data_path = path.join(base_path, "ontime")
delta_table_path = path.join(data_path, "delta-table")


if __name__ == "__main__":
    builder = (
        SparkSession.builder.master("local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    if path.exists(delta_table_path):
        shutil.rmtree(delta_table_path)

    files = [f for f in os.listdir(data_path) if f.endswith(".csv.gz")]
    for index, file in enumerate(sorted(files)):
        file_full_path = os.path.join(data_path, file)
        _df = spark.read.format("csv").option("header", "true").load(file_full_path)
        if index == 0:
            df = _df
            df.write.format("delta").mode("overwrite").save(delta_table_path)
        else:
            df = df.union(_df)
            df.write.format("delta").mode("append").save(delta_table_path)
