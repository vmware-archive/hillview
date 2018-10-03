#!/usr/bin/env python3

"""This Python program stops the Hillview service on the machines specified in the
   configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import ClusterConfiguration, get_config

def stop_webserver(config):
    """Stops the Hillview web server"""
    assert isinstance(config, ClusterConfiguration)
    rh = config.get_webserver()
    print("Stopping web server", rh)
    rh.run_remote_shell_command(
        config.service_folder + "/" + config.tomcat + "/bin/shutdown.sh")
    rh.run_remote_shell_command(
        # The true is there to ignore the exit code of pkill
        "pkill -f Bootstrap; true")

def stop_worker(rh):
    """Stops a Hillview service on a remote worker machine"""
    print("Stopping", rh.host)
    rh.run_remote_shell_command(
        # The true is there to ignore the exit code of pkill
        "pkill -f hillview-server; true")

def stop_backends(config):
    """Stops all Hillview workers"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_workers(stop_worker, True)
    config.run_on_all_aggregators(stop_worker, True)

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = get_config(parser, args)
    stop_webserver(config)
    stop_backends(config)

if __name__ == "__main__":
    main()
