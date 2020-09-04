#!/bin/bash
# This scripts starts the Hillview front-end service locally

mydir="$(dirname -- "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
source ${mydir}/lib.sh

# Download apache if not there.
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

# If you add this line the java process enables a remote debugger to be connected to it
# on port 5005
export JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

# If you want GRPC logging uncomment the following line.
# LOGGING=" -Djava.util.logging.config.file=logging.properties"
export JAVA_OPTS="$JAVA_OPTS$LOGGING"
export CATALINA_PID="catalina.pid"

cd ${mydir}/.. || exit 1
# Create a bookmark directory for local machine
[[ -d bookmark ]] || mkdir bookmark

# Uncomment the following to debug class loader issues
# DEBUG_CLASSLOADER="-verbose:class"
CATALINA_OPTS=${DEBUG_CLASSLOADER} ./apache-tomcat-${TOMCATVERSION}/bin/catalina.sh run
