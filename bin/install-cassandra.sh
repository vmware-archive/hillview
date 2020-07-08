SAVEDIR=$PWD
# Change this ROOR_DIR to your preferred instalation directory, and make sure to update cassandraRootDir at SSTableTest.java
ROOT_DIR="/tmp/"

[ -e $ROOT_DIR/apache-cassandra-3.11.6-bin.tar.gz ] && rm $ROOT_DIR/apache-cassandra-3.11.6-bin.tar.gz
[ -e $ROOT_DIR/cassandra ] && rm -rf $ROOT_DIR/cassandra

# Download Cassandra to $ROOT_DIR
wget https://downloads.apache.org/cassandra/3.11.6/apache-cassandra-3.11.6-bin.tar.gz -O $ROOT_DIR/apache-cassandra-3.11.6-bin.tar.gz
tar xzvf $ROOT_DIR/apache-cassandra-3.11.6-bin.tar.gz -C $ROOT_DIR
mv $ROOT_DIR/apache-cassandra-3.11.6 $ROOT_DIR/cassandra
mkdir $ROOT_DIR/cassandra/logs

# Install Java 8 [Run this if you don't have it]
# sudo apt-get update
# printf "Y" | sudo apt-get install openjdk-8-jdk

# Start Cassandra
$ROOT_DIR/cassandra/bin/cassandra

# Generate 100 flights data (adding unique primary key) from the existing one
python3 ../data/sstable/generate-data.py

echo "Waiting for 10s to let Cassandra finish the initialization"
sleep 10

# Check Cassandra status
$ROOT_DIR/cassandra/bin/nodetool status

# Load data to Cassandra
if [[ $(wc -l < ../data/sstable/cassdb.cql) -le 93 ]]; then
    # this will append a relative path to load the csv data
    echo " FROM '$SAVEDIR/../data/sstable/flights_data.csv' WITH DELIMITER=',' AND HEADER=TRUE; exit;" >> ../data/sstable/cassdb.cql
fi

$ROOT_DIR/cassandra/bin/cqlsh --file ../data/sstable/cassdb.cql

# Force Compaction to put the data into sstable
$ROOT_DIR/cassandra/bin/nodetool flush
$ROOT_DIR/cassandra/bin/nodetool compact cassdb
echo "============================================================================"
echo "Cassandra is successfully installed !"
echo "When done testing, kill Cassandra using: 'pgrep -f cassandra | xargs kill -9'"
echo "Set cassandraRootDir at SSTableTest.java to '$ROOT_DIR/cassandra'"

cd ${SAVEDIR}
