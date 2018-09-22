#!/usr/bin/env python3

"""This Python program stops the Hillview service on the machines specified in the
   configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import RemoteHost, run_on_all_backends, load_config

def stop_webserver(config):
    """Stops the Hillview web server"""
    print("Stopping web server", config.webserver)
    rh = RemoteHost(config.user, config.webserver)
    rh.run_remote_shell_command(
        config.service_folder + "/" + config.tomcat + "/bin/shutdown.sh")
    rh.run_remote_shell_command(
        # The true is there to ignore the exit code of pkill
        "pkill -f Bootstrap; true")

def stop_backend(rh):
    """Stops a Hillview service on a remote machine"""
    print("Stopping backend", rh.host)
    rh.run_remote_shell_command(
        # The true is there to ignore the exit code of pkill
        "pkill -f hillview-server; true")

def stop_backends(config):
    """Stops all Hillview backend workers"""
    run_on_all_backends(config, stop_backend, True)

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = load_config(args.config)
    stop_webserver(config)
    stop_backends(config)

if __name__ == "__main__":
    main()
