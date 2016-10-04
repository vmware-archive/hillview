#!/bash

SPARK_URL="http://d3kbcqa49mib13.cloudfront.net/spark-2.0.0-bin-hadoop2.7.tgz"
HDFS_URL="http://apache.claz.org/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz"
PWD=`pwd`

function MSG {
   echo ""
   echo "##################"
   echo $1
   echo "##################"
   echo ""
}  

MSG "Installing necessary packages. Enter password for sudo"

#
# First, we install some packages. The script will prompt for a password
# to use with sudo.
#
sudo apt-get install -y puppet

if [[ $? > 0 ]]
then
   MSG "Installation failed because apt-get could not fetch packages"
   exit
else
   MSG "apt-get commands successful, continuing."
fi

wget $SPARK_URL
wget $HDFS_URL
 
tar -xzvf spark*.tgz
tar -xzvf hadoop*.tar.gz

MSG "Downloads complete. Rejoice, for we shall now set you up a cluster."
