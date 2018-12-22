#!/usr/bin/env python3

"""This python starts the Hillview service on the machines
   specified in the configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import RemoteHost, RemoteAggregator, ClusterConfiguration, get_config
from hillviewConsoleLog import get_logger

logger = get_logger("start")

def start_webserver(config):
    """Starts the Hillview web server"""
    assert isinstance(config, ClusterConfiguration)
    rh = config.get_webserver()
    message = "Starting web server " + str(rh)
    logger.info(message)
    rh.run_remote_shell_command(
        "export WEB_CLUSTER_DESCRIPTOR=serverlist; cd " + config.service_folder + "; nohup " + \
        config.tomcat + "/bin/startup.sh &")

def start_worker(config, rh):
    """Starts the Hillview worker on a remote machine"""
    assert isinstance(rh, RemoteHost)
    assert isinstance(config, ClusterConfiguration)
    message = "Starting worker " + str(rh)
    logger.info(message)
    gclog = config.service_folder + "/hillview/gc.log"
    rh.run_remote_shell_command(
        "cd " + config.service_folder + "/hillview; " + \
        "nohup java -ea -Dlog4j.configurationFile=./log4j.properties -server " + \
        " -Xmx" + rh.heapsize + " -Xloggc:" + gclog + \
        " -jar " + config.service_folder + \
        "/hillview/hillview-server-jar-with-dependencies.jar " + "0.0.0.0:" + \
        str(config.worker_port) + " >nohup.out 2>&1 &")

def start_aggregator(config, agg):
    """Starts a Hillview aggregator"""
    assert isinstance(agg, RemoteAggregator)
    assert isinstance(config, ClusterConfiguration)
    message = "Starting aggregator " + str(agg)
    logger.info(message)
    agg.run_remote_shell_command(
        "cd " + config.service_folder + "/hillview; " + \
        "nohup java -ea -Dlog4j.configurationFile=./log4j.properties -server " + \
        " -jar " + config.service_folder + \
        "/hillview/hillview-server-jar-with-dependencies.jar " + \
        config.service_folder + "/workers " + agg.host + ":" + \
        str(config.aggregator_port) + " >nohup.agg 2>&1 &")

def start_aggregators(config):
    """Starts all Hillview aggregators"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_aggregators(lambda rh: start_aggregator(config, rh))

def start_workers(config):
    """Starts all Hillview workers"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_workers(lambda rh: start_worker(config, rh))

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = get_config(parser, args)
    start_webserver(config)
    start_workers(config)
    start_aggregators(config)

if __name__ == "__main__":
    main()
