#!/bin/bash
# This shell script downloads some testing data for Hillview and munges it
# using a Java program

set -e

export MAVEN_OPTS="-Xmx2048M"

pushd ../data/ontime
./download.py
popd

while getopts "g" opt; do
  case "$opt" in
    g)
      python3 generate-delta-table.py
      ;;
    *)
      echo "demo-data-cleaner.sh [-g]"
      echo "-g: Generating delta table for testing, requires python3, pyspark, and delta-spark"
      exit 1
      ;;
  esac
done

pushd ../platform
java -jar target/data-cleaner-jar-with-dependencies.jar
popd
