# This folder contains the web part of the Hiero project:

- web services
- web client
The web services part links with the jars produced by the hieroplatform project.

# Installation

## Install Apache Tomcat web application server

Use version 8.5.8.  Download the binaries from
[http://tomcat.apache.org/download-80.cgi] and untar in the hiero
toplevel folder.

$> cd apache-tocat-8.5.8/webapps
$> rm -rf ROOT
$> ln -s ../hieroweb/target/hieroweb-1.0-SNAPSHOT.war ROOT.war

## Install typescript and JavaScript libraries

$> sudo npm install -g typescript ts-loader webpack@1.4.0 typings
$> cd src/main/webapp
$> npm install rx rx-dom
$> typings install dt~rx-dom --save

## Building

$> cd hieroweb
$> mvn package

## Running the web ui

* Run the Tomcat web server

$> ./apache-tomcat-8.5.8/bin/catalina.sh run

* Open a browser at [http://localhost:8080]
