#!/bin/sh
# This script runs npm install

cd src/main/webapp
rm node_modules/typescript
npm install
npm link typescript
cd ../../../
