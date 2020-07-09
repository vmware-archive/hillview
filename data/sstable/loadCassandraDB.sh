#!/bin/bash
# This shell script triggers data generator and then imports the data to 
# local Cassandra instance using cqlsh's COPY command

set -e
SAVEDIR=$PWD
source ../../bin/lib.sh

# Prepare the data
./generate-data.py

# this will append a relative path to load the csv data
if [[ $(wc -l < cassdb.cql) -le 93 ]]; then
    echo " FROM '$SAVEDIR/flights_data.csv' WITH DELIMITER=',' AND HEADER=TRUE; exit;" >> cassdb.cql
fi

# insert the data to Cassandra 
$CASSANDRA_INSTALLATION_DIR/cassandra/bin/cqlsh --file $SAVEDIR/cassdb.cql

# Force Compaction to put the data into sstable
$CASSANDRA_INSTALLATION_DIR/cassandra/bin/nodetool flush
$CASSANDRA_INSTALLATION_DIR/cassandra/bin/nodetool compact cassdb
