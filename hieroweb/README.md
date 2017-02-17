# This folder contains the web part of the Hiero project:

- web services
- web client
The web services part links with the jars produced by the hieroplatform project.

# Installation

## Install typescript and JavaScript libraries

$> sudo npm install -g typescript ts-loader webpack@1.4.0 typings
$> cd src/main/webapp
$> npm install rx rx-dom
$> typings install dt~rx-dom --save

## Install Apache Tomcat web application server

Use version 8.5.8.  Download the binaries from
[http://tomcat.apache.org/download-80.cgi] and untar in the hiero
toplevel folder.

## Building

$> cd hieroweb
$> mvn package

## Running the web ui

[TODO]
* Run Tomcat
* Open a browser at [http://localhost:8080]
