#!/usr/bin/env python3

"""This python starts the Hillview service on the machines
   specified in the configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import RemoteHost, load_config, run_on_all_backends

def start_webserver(config):
    """Starts the Hillview web server"""
    print("Starting web server", config.webserver)
    rh = RemoteHost(config.user, config.webserver)
    rh.run_remote_shell_command(
        "export WEB_CLUSTER_DESCRIPTOR=serverlist; cd " + config.service_folder + "; nohup " + \
        config.tomcat + "/bin/startup.sh &")

def start_backend(config, rh):
    """Starts the Hillview service on a remote machine"""
    assert isinstance(rh, RemoteHost)
    print("Starting backend", rh)
    gclog = config.service_folder + "/hillview/gc.log"
    if rh.host in config.backends_heapsize:
        heapsize = config.backends_heapsize[rh.host]
    else:
        heapsize = config.default_heap_size
    rh.run_remote_shell_command(
        "cd " + config.service_folder + "/hillview; " + \
        "nohup java -Dlog4j.configurationFile=./log4j.properties -server -Xms" + heapsize + \
        " -Xmx" + heapsize + " -Xloggc:" + gclog + \
        " -jar " + config.service_folder + \
        "/hillview/hillview-server-jar-with-dependencies.jar 0.0.0.0:" + \
        str(config.backend_port) + " >nohup.out 2>&1 &")
    # Check to see whether the remote service is still running.  Sometimes it fails right away
    rh.run_remote_shell_command("if pgrep -f hillview-server; then echo Started; else " +
                                " echo \"Could not start hillview on " + str(rh.host) +"\"; " +
                                " cat " + config.service_folder + "/hillview/nohup.out; false; fi")

def start_backends(config):
    """Starts all Hillview backend workers"""
    run_on_all_backends(config, lambda rh: start_backend(config, rh), True)

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = load_config(args.config)
    start_webserver(config)
    start_backends(config)

if __name__ == "__main__":
    main()
