#!/bin/bash

# Helper script to run webpack from maven

echo "Running webpack command in src/main/webapp"
cd src/main/webapp/
rm -rf bundle*

webpack --display-modules

