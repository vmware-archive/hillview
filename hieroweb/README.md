# This folder contains the web part of the hiero project:
- web services
- web client
The web services part links with the jars produced by the hieroplatform project.

# Installation

## Install typescript and JavaScript libraries

sudo npm install -g typescript ts-loader webpack typings

## Install type information for TypeScript compiler

typings install dt~rx-dom --global --save

## Install Apache Tomcat web application server

Use version 8.5.8.
Download the binaries from http://tomcat.apache.org/download-80.cgi and untar in the hiero toplevel
folder.
