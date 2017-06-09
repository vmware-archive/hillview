#!/bin/bash

# Helper script to run webpack from maven

echo "Running webpack command in src/main/webapp"
cd src/main/webapp/
rm -rf bundle*

# Webpack is so stupid that it returns 0 even if there are errors!
tmpfile=$(mktemp webpack-out.XXXXXX)
webpack --display-modules >$tmpfile
cat $tmpfile
if grep -i error $tmpfile
then
    rm -f $tmpfile
    echo "Webpack command failed"
    exit 1
fi
rm -f $tmpfile

