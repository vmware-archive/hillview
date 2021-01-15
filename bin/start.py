#!/usr/bin/env python
# We attempted to make this program work with both python2 and python3

"""This python script starts the Hillview service on the machines
   specified in the configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import RemoteHost, RemoteAggregator, ClusterConfiguration, get_config, get_logger

logger = get_logger("start")

def start_webserver(config):
    """Starts the Hillview web server"""
    assert isinstance(config, ClusterConfiguration)
    rh = config.get_webserver()
    message = "Starting web server " + str(rh)
    logger.info(message)
    rh.run_remote_shell_command(config.service_folder + "/hillview-webserver-manager.sh start")

def start_worker(config, rh):
    """Starts the Hillview worker on a remote machine"""
    assert isinstance(rh, RemoteHost)
    assert isinstance(config, ClusterConfiguration)
    message = "Starting worker " + str(rh)
    logger.info(message)
    rh.run_remote_shell_command(config.service_folder + "/hillview-worker-manager.sh start")

def start_aggregator(config, agg):
    """Starts a Hillview aggregator"""
    assert isinstance(agg, RemoteAggregator)
    assert isinstance(config, ClusterConfiguration)
    message = "Starting aggregator " + str(agg)
    logger.info(message)
    agg.run_remote_shell_command(config.service_folder + "/hillview-aggregator-manager.sh start")

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = get_config(parser, args)
    start_webserver(config)
    config.run_on_all_aggregators(lambda rh: start_aggregator(config, rh))
    config.run_on_all_workers(lambda rh: start_worker(config, rh))

if __name__ == "__main__":
    main()
