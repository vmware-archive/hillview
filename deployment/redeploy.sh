#!/bin/sh
# Compile and deploy program on fast cluster

SKIP=""
ONECLOUD=""
while getopts soh FLAG; do
   case $FLAG in
      s) SKIP="1"
         ;;
      o) ONECLOUD="-o";
         ;;
      h) echo "redeploy.sh [-osh]"
         echo "Rebuild and redeploy program on a cluster"
         echo "-o: redeploy on OneCloud, else on fast cluster"
         echo "-s: skip rebuilding, just redeploy"
         echo "-h: help"
         exit 1
         ;;
   esac
done

./prepare.sh $ONECLOUD

if [ -z $ONECLOUD ]; then
    EXTRAARGS="-i fast-hosts"
else
    EXTRAARGS="-i hosts -u hillview"
fi

if [ -z $SKIP ]; then
    cd ../platform
    mvn -DskipTets install
    cd ../web
    mvn package
    cd ../deployment
fi

echo "Stopping services"
ansible-playbook stop.yaml $EXTRAARGS
echo "Installing software"
ansible-playbook prepare.yaml $EXTRAARGS
echo "Starting services"
ansible-playbook start.yaml $EXTRAARGS
