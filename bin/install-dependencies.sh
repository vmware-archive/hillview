#!/bin/bash
# This program installs the software needed to build Hillview

# Bail out at first error
set -e
set -x

usage() {
    echo "install-dependencies.sh [-h][-u]"
    echo "Install dependencies needed by Hillview"
    echo "-u: Install only dependencies needed to run Hillview (not to develop)"
    echo "-h: help"
    exit 1
}

# If this is 1 we only install dependencies needed for using Hillview,
# but not for developing Hillview
USERDEPS=0
while getopts uh FLAG; do
   case ${FLAG} in
      u) USERDEPS=1  # no dependecies for developers
        echo "User dependencies only"
         ;;
      h) usage
         ;;
      *) usage
         ;;
   esac
done

# Set to 0 if you don't want to install cassandra locally for tests
INSTALL_CASSANDRA=1
SAVEDIR=${PWD}
mydir="$(dirname "$0")"
if [[ ! -d "${mydir}" ]]; then mydir="${PWD}"; fi
source ${mydir}/lib.sh

echo "Installing programs needed to build"

case "${OSTYPE}" in
    linux*)
    # Location where node.js version 11 resides.
        echo "Installing curl"
        ${SUDO} ${INSTALL} install curl -y
	curl -sL https://deb.nodesource.com/setup_12.x | ${SUDO} -E bash -
esac

${SUDO} ${INSTALL} install wget maven ${NODEJS} ${NPM} ${LIBFORTRAN} unzip gzip
echo "Installing typescript compiler"
${SUDO} npm install -g typescript@3.9

pushd ..
if [ ! -d apache-tomcat-${TOMCATVERSION} ]; then
    echo "Installing apache Tomcat web server"
    wget http://archive.apache.org/dist/tomcat/tomcat-9/v${TOMCATVERSION}/bin/apache-tomcat-${TOMCATVERSION}.tar.gz
    tar xvfz apache-tomcat-${TOMCATVERSION}.tar.gz
    cd apache-tomcat-${TOMCATVERSION}/webapps
    rm -rf ROOT* examples docs manager host-manager
    ln -s ../../web/target/web-1.0-SNAPSHOT.war ROOT.war
    cd ../..
    rm -rf apache-tomcat-${TOMCATVERSION}.tar.gz
else
    echo "Tomcat already installed"
fi
popd

if [ ${USERDEPS} -eq 0 ]; then
  echo "Downloading test data"
  cd ${mydir}/../data/ontime
  ./download.py
  cd ../ontime_private
  ./gen_metadata.py
  cd ../..
fi

cd web/src/main/webapp
echo "Installing Javascript packages"
rm -f node_modules/typescript
npm install
npm link typescript
cd ${SAVEDIR}

if [ ${USERDEPS} -eq 0 ]; then
  if [ ${INSTALL_CASSANDRA} -eq 1 ]; then
      ./${mydir}/install-cassandra.sh
  fi
fi