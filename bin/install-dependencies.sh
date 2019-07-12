#!/bin/bash
# This program installs the software needed to build Hillview

# Bail out at first error
set -e
set -x

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
source ${mydir}/lib.sh

echo "Installing programs needed to build"

case "$OSTYPE" in
    linux*)
    # Location where node.js version 11 resides.
        echo "Installing curl"
        ${SUDO} ${INSTALL} install curl -y
	curl -sL https://deb.nodesource.com/setup_11.x | ${SUDO} -E bash -
esac
${SUDO} ${INSTALL}  install wget maven ${NODEJS} ${NPM} ${LIBFORTRAN} unzip gzip
echo "Installing typescript compiler"
${SUDO} npm install -g typescript@3.1.5

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
rm -f node_modules/typescript
npm install
npm link typescript
popd
