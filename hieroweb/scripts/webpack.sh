#!/bin/bash

echo "Running webpack command in src/main/webapp"
cd src/main/webapp/
rm -rf bundle*

if webpack | grep ERROR
then
    echo "Webpack command failed"
    exit 1
fi
