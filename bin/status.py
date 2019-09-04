#!/usr/bin/env python3

"""This program checks if the Hillview service is running on all machines
   specified in the configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import ClusterConfiguration, RemoteHost, get_config, get_logger

logger = get_logger("status")

def check_webserver(config):
    """Checks if the Hillview web server is running"""
    assert isinstance(config, ClusterConfiguration)
    rh = config.get_webserver()
    message = "Checking hillview status on " + str(rh)
    logger.info(message)
    rh.run_remote_shell_command(config.service_folder + "/hillview-webserver-manager.sh status")

def check_worker(config, rh):
    """Checks if the Hillview service is running on a remote machine"""
    assert isinstance(config, ClusterConfiguration)
    assert isinstance(rh, RemoteHost)
    message = "Checking hillview status on " + str(rh.host)
    logger.info(message)
    rh.run_remote_shell_command(config.service_folder + "/hillview-worker-manager.sh status")

def check_aggregator(config, rh):
    """Checks if the Hillview service is running on a remote machine"""
    assert isinstance(config, ClusterConfiguration)
    assert isinstance(rh, RemoteHost)
    message = "Checking hillview status on " + str(rh.host)
    logger.info(message)
    rh.run_remote_shell_command(config.service_folder + "/hillview-aggregator-manager.sh status")

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = get_config(parser, args)
    check_webserver(config)
    config.run_on_all_aggregators(lambda rh: check_aggregator(config, rh))
    config.run_on_all_workers(lambda rh: check_worker(config, rh))

if __name__ == "__main__":
    main()
