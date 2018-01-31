#!/bin/bash
# This program installs the software needed to build Hillview

# Bail out at first error
set -e

echo "Installing programs needed to build"
sudo apt-get install maven nodejs-legacy ansible npm
echo "Installing typescript tools"
sudo npm install -g typescript ts-loader webpack

TOMCATVERSION="9.0.4"
cd ..
if [ ! -f apache-tomcat-${TOMCATVERSION} ]; then
    echo "Installing apache Tomcat web server"
    wget http://archive.apache.org/dist/tomcat/tomcat-9/v${TOMCATVERSION}/bin/apache-tomcat-${TOMCATVERSION}.tar.gz
    tar xvfz apache-tomcat-${TOMCATVERSION}.tar.gz
    pushd apache-tomcat-${TOMCATVERSION}/webapps
    rm -rf ROOT*
    ln -s ../../web/target/web-1.0-SNAPSHOT.war ROOT.war
    popd
else
    echo "Tomcat already installed"
fi

pushd web/src/main/webapp
echo "Installing Javascript packages"
npm install
popd
