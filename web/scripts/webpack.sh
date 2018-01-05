#!/bin/bash

# Helper script to run webpack from maven

pushd src/main/webapp/
echo "Installing npm packages"
npm install
rm -rf bundle*
echo "Running webpack command in src/main/webapp"
webpack --display-modules
popd
