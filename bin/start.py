#!/usr/bin/env python3

# This python starts the Hillview service on the machines
# specified in the configuration file.

from hillviewCommon import *
from optparse import OptionParser

def start_webserver(config):
    """Starts the Hillview web server"""
    print("Starting web server", config.webserver)
    rh = RemoteHost(config.user, config.webserver)
    rh.run_remote_shell_command(
        "export WEB_CLUSTER_DESCRIPTOR=serverlist; cd " + config.service_folder + "; nohup " + \
        config.tomcat + "/bin/startup.sh &")

def start_backend(config, host):
    """Starts the Hillview service on a remote machine"""
    print("Starting backend", host)
    gclog = config.service_folder + "/hillview/gc.log"
    if host in config.backends_heapsize:
        heapsize = config.backends_heapsize[host]
    else:
        heapsize = config.default_heap_size
    rh = RemoteHost(config.user, host)
    rh.run_remote_shell_command(
        "cd " + config.service_folder + "/hillview; " + \
        "nohup java -Dlog4j.configurationFile=./log4j.properties -server -Xms" + heapsize + \
        " -Xmx" + heapsize + " -Xloggc:" + gclog + \
        " -jar " + config.service_folder + "/hillview/hillview-server-jar-with-dependencies.jar 0.0.0.0:" + \
        str(config.backend_port) + ">/dev/null 2>/dev/null &")

def start_backends(config):
    """Starts all Hillview backend workers"""
    for h in config.backends:
        start_backend(config, h)

def main():
    parser = OptionParser(usage="%prog config_file")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        usage(parser)
    config = load_config(parser, args[0])
    start_webserver(config)
    start_backends(config)

if __name__ == "__main__":
    main()
