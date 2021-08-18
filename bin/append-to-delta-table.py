#!/usr/bin/env python3
import argparse
import sys
import textwrap
from enum import Enum, unique

try:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
except ModuleNotFoundError:
    print(textwrap.dedent("""\
    This script requires pyspark and delta-spark.
    You can install these modules using the following command:
        pip install --user pyspark delta-spark
    """), file=sys.stderr)
    sys.exit(-1)

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description=textwrap.dedent("""
    Append data from an input file to a delta table.
    If a delta table doesn't exist at the provided path, a new table will be created."""))
parser.add_argument("input_file", help="Path to the input file.")
parser.add_argument("delta_table_path", help="Path to the delta table.")
# TODO: make the format parameter optional, the script will have to auto detect the format
parser.add_argument("-f", "--format", metavar="FORMAT", required=True, help="Format of the input file.")
parser.add_argument("-o", "--option", action="append", nargs=2, metavar=("KEY", "VALUE"), help=textwrap.dedent("""\
    Additional options to use when loading the input file.
    This argument can be repeated multiples to specify multiple options"""))


@unique
class Format(Enum):
    JSON = "json"
    CSV = "csv"
    ORC = "orc"
    PARQUET = "parquet"


if __name__ == "__main__":
    args = parser.parse_args()
    input_file = args.input_file
    delta_table_path = args.delta_table_path

    try:
        input_file_format = Format(args.format)
    except ValueError:
        print(f"'{args.format}' is not a valid format, supported formats are f{[f.value for f in Format]}",
              file=sys.stderr)
        sys.exit(-1)

    builder = (
        SparkSession.builder.master("local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    loader = spark.read.format(input_file_format.value)
    for (key, value) in args.option:
        loader = loader.option(key, value)
    df = loader.load(input_file)
    df.write.format("delta").mode("append").save(delta_table_path)
