# This folder contains the web part of the Hiero project:

- web services
- web client
The web services part links with the jars produced by the hieroplatform project.

# Installation

## Install Apache Tomcat web application server

Use version 8.5.8.  Download the binaries from
[http://tomcat.apache.org/download-80.cgi] and untar in the hiero
toplevel folder.

```
$ cd apache-tocat-8.5.8/webapps
$ rm -rf ROOT
$ ln -s ../../hieroweb/target/hieroweb-1.0-SNAPSHOT.war ROOT.war
```

## Install typescript and JavaScript libraries and tools

First command may need sudo; on Mac it seems to work without sudo.

```
$ sudo npm install -g typescript ts-loader webpack@1.14.0 typings
```

This installs the typescript compiler, the webpack tool which can be used to bundle 
multiple JavaScript files together, the ts-loader tool for webpack, which allows it to 
compile directly javascript, and the typings tool, which can be used to install
typescript type definition files for JavaScript code.   Then using 

Then we install various JavaScript libraries that we use: rx, rx-dom and d3, together with
the typescript type definitions for these libraries.

```
$ cd src/main/webapp
$ npm install rx rx-dom d3
$ npm install @types/d3 --save
$ typings install dt~rx-dom --save
$ cd ../../..
```

## Building

```
$ cd hieroweb
$ mvn package
```

## Running the web ui

* Run the Tomcat web server

```
$ ../apache-tomcat-8.5.8/bin/catalina.sh run
```

* Open a browser at [http://localhost:8080]
