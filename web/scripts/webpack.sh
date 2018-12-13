#!/bin/bash

set -e

# Helper script to run webpack from maven

pushd src/main/webapp/
#echo "Installing npm packages"
#npm install
rm -rf bundle*
echo "Running webpack command in src/main/webapp"
./node_modules/.bin/webpack --display-modules
mv dist/bundle.js .
mv dist/bundle.js.map .
popd
