SAVEDIR=$PWD

[ -e /tmp/apache-cassandra-3.11.6-bin.tar.gz ] && rm /tmp/apache-cassandra-3.11.6-bin.tar.gz 
[ -e /tmp/cassandra ] && rm -rf /tmp/cassandra 

# Download Cassandra to /tmp
wget https://downloads.apache.org/cassandra/3.11.6/apache-cassandra-3.11.6-bin.tar.gz -O /tmp/apache-cassandra-3.11.6-bin.tar.gz
tar xzvf /tmp/apache-cassandra-3.11.6-bin.tar.gz -C /tmp
mv /tmp/apache-cassandra-3.11.6 /tmp/cassandra
mkdir /tmp/cassandra/logs

# Install Java 8 [Run this if you don't have it] 
# sudo apt-get update
# printf "Y" | sudo apt-get install openjdk-8-jdk

# Start Cassandra
/tmp/cassandra/bin/cassandra

echo "Waiting for 10s to let Cassandra finish the initialization"
sleep 10

# Check Cassandra status 
/tmp/cassandra/bin/nodetool status

# Load data to Cassandra
if [[ $(wc -l < ../data/sstable/cassdb.cql) -le 93 ]]; then
    # this will append a relative path to load the csv data  
    echo "FROM '$SAVEDIR/../data/sstable/flights_sample.csv' WITH DELIMITER=',' AND HEADER=TRUE; exit;" >> ../data/sstable/cassdb.cql
fi
/tmp/cassandra/bin/cqlsh --file ../data/sstable/cassdb.cql

# Force Compaction to put the data into sstable 
/tmp/cassandra/bin/nodetool flush
/tmp/cassandra/bin/nodetool compact cassdb 

echo "When done testing, kill Cassandra using: pgrep -f cassandra | xargs kill -9 "
echo "Cassandra is successfully installed"
cd ${SAVEDIR}
