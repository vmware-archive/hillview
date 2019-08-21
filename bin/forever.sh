#!/bin/sh
# Runs another process in a loop

if [ $# -le 1 ]; then
    echo "Usage: forever.sh pidfile command"
    echo "Re-runs command every time it terminates"
    echo "Writes its own pid in pidfile"
    exit 1
fi

pidfile=$1
shift

echo "Running $1 forever"
echo $$ >${pidfile}
while /bin/true; do
    $*
    sleep 2
    echo "Restarting..."
done
