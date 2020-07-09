#!/bin/bash
# This script will install Cassandra at $CASSANDRA_INSTALLATION_DIR/cassandra/ and start it. The process is as follows:
# 1. Download apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz (if not exist) and then extract it
# 2. Start Cassandra instance locally
# 3. Waiting the initialization to complete for 15 seconds

SAVEDIR=$PWD
# Change this to your preferred installation directory, and make sure to update cassandraRootDir at SSTableTest.java
CASSANDRA_INSTALLATION_DIR="/tmp"
CASSANDRA_VERSION="3.11.6"

# Download Cassandra and extract it to $CASSANDRA_INSTALLATION_DIR/cassandra

if [ ! -d $CASSANDRA_INSTALLATION_DIR"/cassandra" ]; then
    # Only download/extract when cassandra/ doesn't exist
    [[ -f $CASSANDRA_INSTALLATION_DIR/apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz ]] || wget https://downloads.apache.org/cassandra/$CASSANDRA_VERSION/apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz -O $CASSANDRA_INSTALLATION_DIR/apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz
    [[ -d $CASSANDRA_INSTALLATION_DIR/cassandra ]] || { tar xzvf $CASSANDRA_INSTALLATION_DIR/apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz -C $CASSANDRA_INSTALLATION_DIR ; mv $CASSANDRA_INSTALLATION_DIR/apache-cassandra-$CASSANDRA_VERSION $CASSANDRA_INSTALLATION_DIR/cassandra ; }
fi

# Create a logging directory for Cassandra
[[ -d $CASSANDRA_INSTALLATION_DIR/cassandra/logs ]] || mkdir $CASSANDRA_INSTALLATION_DIR/cassandra/logs

# Install Java 8 [Run this if you don't have it]
# sudo apt-get update
# printf "Y" | sudo apt-get install openjdk-8-jdk

# Start Cassandra
$CASSANDRA_INSTALLATION_DIR/cassandra/bin/cassandra

echo "Waiting for 15s to let Cassandra finish the initialization"
sleep 15
echo "============================================================================"
echo "Cassandra is successfully installed !"
echo "When done testing, kill Cassandra using: 'pgrep -f cassandra | xargs kill -9'"
echo "Set cassandraRootDir at SSTableTest.java to '$CASSANDRA_INSTALLATION_DIR/cassandra'"

cd ${SAVEDIR}
