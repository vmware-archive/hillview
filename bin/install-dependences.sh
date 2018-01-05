#!/bin/bash
# This program installs the software needed to build Hillview

# Bail out at first error
set -e

echo "Installing programs needed to build"
sudo apt-get install maven nodejs-legacy ansible npm
echo "Installing typescript tools"
sudo npm install -g typescript ts-loader webpack

cd ..
if [ -f apache-tomcat-8.5.8 ]; then
    echo "Instsalling apache Tomcat web server"
    wget http://archive.apache.org/dist/tomcat/tomcat-8/v8.5.8/bin/apache-tomcat-8.5.8.tar.gz
    tar xvfz apache-tomcat-8.5.8.tar.gz
    pushd apache-tomcat-8.5.8/webapps
    rm -rf ROOT*
    ln -s ../../web/target/web-1.0-SNAPSHOT.war ROOT.war
    popd
fi

pushd web/src/main/webapp
echo "Installing Javascript packages"
npm install
popd
