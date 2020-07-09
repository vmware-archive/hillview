#!/bin/bash
# This shell script downloads some testing data for Hillview and munges it
# using a Java program

set -e

export MAVEN_OPTS="-Xmx2048M"
pushd ../data/ontime
./download.py
popd
pushd ../platform
java -jar target/data-cleaner-jar-with-dependencies.jar
popd

# Insert testing data to local Cassandra
pushd ../data/sstable
./loadCassandraDB.sh
popd
