#!/usr/bin/env python3

# This Python program stops the Hillview service on the machines specified in the
# configuration file.

from hillviewCommon import *
from optparse import OptionParser

def stop_webserver(config):
    """Stops the Hillview web server"""
    print("Stopping web server", config.webserver)
    rh = RemoteHost(config.user, config.webserver)
    rh.run_remote_shell_command(
        config.service_folder + "/" + config.tomcat + "/bin/shutdown.sh")
    rh.run_remote_shell_command(
        # The echo is there to ignore the exit code of pkill
        "pkill -f Bootstrap; echo")

def stop_backend(config, host):
    """Stops a Hillview service on a remote machine"""
    print("Stopping backend", host)
    rh = RemoteHost(config.user, host)
    rh.run_remote_shell_command(
        # The echo is there to ignore the exit code of pkill
        "pkill -f hillview-server; echo")

def stop_backends(config):
    """Stops all Hillview backend workers"""
    for h in config.backends:
        stop_backend(config, h)

def main():
    parser = OptionParser(usage="%prog config_file")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        usage(parser)
    config = load_config(parser, args[0])
    stop_webserver(config)
    stop_backends(config)

if __name__ == "__main__":
    main()
