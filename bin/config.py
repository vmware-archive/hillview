# This file is a Python program that defines the configuration for a
# Hillview deployment.  It is imported as a Python module by other
# Python files that handle the deployment.

# Name of machine hosting the web server
webserver = "web.server.name"

# Names of the machines hosting the workers; the web
# server machine can also act as a worker
backends = [
    "worker1.name",
    "worker2.name" # etc.
]

# This is a Python map which can be used to override the
# default_heap_size value below for specific machines.
backends_heapsize = {
    "worker1.name": "25G"
}

# Network port where the servers listen for requests
backend_port = 3569
# Java heap size for Hillview service
default_heap_size = "25G"
# User account for running the Hillview service
user = "hillview"
# Folder where the hillview service is installed on remote machines
service_folder = "/home/hillview"
# Version of Apache Tomcat to deploy
tomcat_version = "9.0.4"
# Tomcat installation folder name
tomcat = "apache-tomcat-" + tomcat_version
# If true delete old log files
cleanup = False
