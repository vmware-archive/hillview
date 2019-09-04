#!/usr/bin/env python3

"""This Python program stops the Hillview service on the machines specified in the
   configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import ClusterConfiguration, get_config, get_logger

logger = get_logger("stop")

def stop_webserver(config):
    """Stops the Hillview web server"""
    assert isinstance(config, ClusterConfiguration)
    rh = config.get_webserver()
    message = "Stopping web server on " + str(rh)
    logger.info(message)
    rh.run_remote_shell_command(config.service_folder + "/hillview-webserver-manager.sh stop")

def stop_worker(config, rh):
    """Stops a Hillview worker service on a remote machine"""
    rh.run_remote_shell_command(config.service_folder + "/hillview-worker-manager.sh stop")

def stop_aggregator(config, rh):
    """Stops a Hillview aggregator service on a remote machine"""
    rh.run_remote_shell_command(config.service_folder + "/hillview-aggregator-manager.sh stop")

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = get_config(parser, args)
    stop_webserver(config)
    config.run_on_all_workers(lambda rh: stop_worker(config, rh))
    config.run_on_all_aggregators(lambda rh: stop_aggregator(config, rh))

if __name__ == "__main__":
    main()
