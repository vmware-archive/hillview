#!/bin/bash
# This shell script downloads some testing data for Hillview and munges it
# using a Java program

set -e

export MAVEN_OPTS="-Xmx2048M"

pushd ../data/ontime
./download.py
popd

ARGS=""
while getopts "dp" opt; do
  case "$opt" in
    d)
      pushd ../data/ontime/
      if [ ! -d "delta-table" ]; then
        for file in `ls *.csv.gz | sort -n`; do
          python3 ../../bin/append-to-delta-table.py --format csv -o header true $file delta-table
        done
      fi
      popd
      ;;
    p)
      ARGS+="-Dparquet.enabled "
      ;;
    *)
      echo "Usage: demo-data-cleaner.sh [-d] [-p]"
      echo "This script runs the java class DemoDataCleaner to prepare data for testing/demo purposes."
      echo "-d: Also generate a delta table for testing, requires python3, pyspark, and delta-spark."
      echo "-p: Also generate parquet files for testing."
      exit 1
      ;;
  esac
done

pushd ../platform
java -jar ${ARGS} target/data-cleaner-jar-with-dependencies.jar
popd
