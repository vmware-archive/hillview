#!/bin/bash
# This shell script triggers data generator and then imports the data to 
# local Cassandra instance using cqlsh's COPY command

set -e

CASSANDRA_INSTALLATION_DIR="/tmp"
CURR_DIR=$PWD

# Prepare the data
./generate-data.py

# this will append a relative path to load the csv data
if [[ $(wc -l < cassdb.cql) -le 93 ]]; then
    echo " FROM '$CURR_DIR/flights_data.csv' WITH DELIMITER=',' AND HEADER=TRUE; exit;" >> cassdb.cql
fi

# insert the data to Cassandra 
pushd $CASSANDRA_INSTALLATION_DIR/cassandra/bin/
./cqlsh --file $CURR_DIR/cassdb.cql

# Force Compaction to put the data into sstable
./nodetool flush
./nodetool compact cassdb

popd
