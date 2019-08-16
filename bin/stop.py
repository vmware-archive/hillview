#!/usr/bin/env python3

"""This Python program stops the Hillview service on the machines specified in the
   configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import ClusterConfiguration, get_config
from hillviewConsoleLog import get_logger

logger = get_logger("stop")

def stop_webserver(config):
    """Stops the Hillview web server"""
    assert isinstance(config, ClusterConfiguration)
    rh = config.get_webserver()
    message = "Stopping web server on " + str(rh)
    logger.info(message)
    rh.run_remote_shell_command("if pgrep -f tomcat; then " + config.service_folder + "/" +
                                config.tomcat + "/bin/shutdown.sh; echo Stopped; else " +
                                " echo \"Web server already stopped on " + str(rh.host) +"\"; " +
                                " true; fi")
    rh.run_remote_shell_command(
        # The true is there to ignore the exit code of pkill
        "pkill -f Bootstrap; true")

def stop_worker(rh):
    """Stops a Hillview worker service on a remote machine"""
    rh.run_remote_shell_command("hillview-worker-manager.sh stop || pkill -f hillview-server")

def stop_aggregator(rh):
    """Stops a Hillview aggregator service on a remote machine"""
    rh.run_remote_shell_command("hillview-aggregator-manager.sh stop || pkill -f hillview-server")

def stop_backends(config):
    """Stops all Hillview workers"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_workers(stop_worker)
    config.run_on_all_aggregators(stop_aggregator)

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
