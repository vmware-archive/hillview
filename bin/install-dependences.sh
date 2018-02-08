#!/bin/bash
# This program installs the software needed to build Hillview

# Bail out at first error
set -e

source ./config.sh

echo "Installing programs needed to build"
${SUDO} ${INSTALL} install maven nodejs-legacy ansible npm
echo "Installing typescript tools"
${SUDO} npm install -g typescript ts-loader webpack

cd ..
if [ ! -d apache-tomcat-${TOMCATVERSION} ]; then
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
