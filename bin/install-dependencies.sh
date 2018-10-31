#!/bin/bash
# This program installs the software needed to build Hillview

# Bail out at first error
set -e
set -x

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
source $mydir/lib.sh

echo "Installing programs needed to build"
${SUDO} ${INSTALL} install maven ${NODEJS} ${NPM} libgfortran3 unzip gzip
echo "Installing typescript tools"
${SUDO} npm install -g typescript@2.7.1 

cd ..
if [ ! -d apache-tomcat-${TOMCATVERSION} ]; then
    echo "Installing apache Tomcat web server"
    wget http://archive.apache.org/dist/tomcat/tomcat-9/v${TOMCATVERSION}/bin/apache-tomcat-${TOMCATVERSION}.tar.gz
    tar xvfz apache-tomcat-${TOMCATVERSION}.tar.gz
    pushd apache-tomcat-${TOMCATVERSION}/webapps
    rm -rf ROOT*
    ln -s ../../web/target/web-1.0-SNAPSHOT.war ROOT.war
    popd
    rm -rf apache-tomcat-${TOMCATVERSION}.tar.gz
else
    echo "Tomcat already installed"
fi

pushd web/src/main/webapp
echo "Installing Javascript packages"
npm install
npm link typescript
popd
