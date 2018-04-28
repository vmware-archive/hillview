#!/bin/sh
# Compile and deploy program

# Bail out at first error
set -e

usage() {
    echo "redeploy.sh [-shd] config.py"
    echo "Rebuild and redeploy program on a cluster"
    echo "-d: skip deploying software, just stop and start"
    echo "-s: skip rebuilding, just redeploy"
    echo "-h: help"
    exit 1
}

SKIP=""
NOREDEPLOY=""
while getopts sdh FLAG; do
   case $FLAG in
      s) SKIP="1"
         ;;
      d) NOREDEPLOY="1"
         ;;
      *) usage
         ;;
   esac
done
shift $((OPTIND-1))

if [ $# -ne 1 ]; then 
   echo "You must supply a config.py argument"
   usage
fi

CONFIG=$1
shift
echo Using configuration $CONFIG

if [ -z $SKIP ]; then
    cd ../platform
    mvn -DskipTets install
    cd ../web
    mvn package
    cd ../bin
fi

echo "Stopping services"
./stop.py $CONFIG
if [ -z $NOREDEPLOY ]; then
    echo "Installing software"
    ./deploy.py $CONFIG
fi
echo "Starting services"
./start.py $CONFIG
