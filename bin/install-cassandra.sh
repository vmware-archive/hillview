#!/bin/bash
# This script will install Cassandra at $CASSANDRA_INSTALLATION_DIR and start it. 

set -ex

mydir="$(dirname "$0")"
if [[ ! -d "${mydir}" ]]; then mydir="${PWD}"; fi
# shellcheck source=./lib.sh
source ${mydir}/lib.sh

# Download Cassandra and extract it to $CASSANDRA_INSTALLATION_DIR/cassandra
TARFILE=apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz
if [ ! -d ${CASSANDRA_INSTALLATION_DIR} ]; then
    # Only download/extract when cassandra/ doesn't exist
    [[ -f ${TARFILE} ]] || wget https://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION}/${TARFILE}
    [[ -d ${CASSANDRA_INSTALLATION_DIR} ]] || {
        tar xzvf ${TARFILE}
        mv apache-cassandra-${CASSANDRA_VERSION} ${CASSANDRA_INSTALLATION_DIR}
        rm ${TARFILE}
    }
fi

# Create a logging directory for Cassandra
[[ -d ${CASSANDRA_INSTALLATION_DIR}/logs ]] || mkdir ${CASSANDRA_INSTALLATION_DIR}/logs

# Install Java 8 [Run this if you don't have it]
# sudo apt-get update
# printf "Y" | sudo apt-get install openjdk-8-jdk

# Start Cassandra
${CASSANDRA_INSTALLATION_DIR}/bin/cassandra

echo "Waiting for 15s to let Cassandra finish the initialization"
sleep 15
echo "When done testing, kill Cassandra using: pkill -e -f cassandra"
echo "Set cassandraRootDir at SSTableTest.java to '${CASSANDRA_INSTALLATION_DIR}'"
