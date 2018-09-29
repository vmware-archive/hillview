#!/usr/bin/env python3

"""This program checks if the Hillview service is running on all machines
   specified in the configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import ClusterConfiguration, RemoteHost

def check_webserver(config):
    """Checks if the Hillview web server is running"""
    assert isinstance(config, ClusterConfiguration)
    rh = config.get_webserver()
    rh.run_remote_shell_command("if pgrep -f tomcat; then true; else " +
                                " echo \"Web server not running on " + str(rh.host) +"\"; " +
                                " false; fi")

def check_worker(config, rh):
    """Checks if the Hillview service is running on a remote machine"""
    assert isinstance(config, ClusterConfiguration)
    assert isinstance(rh, RemoteHost)
    rh.run_remote_shell_command("if pgrep -f hillview-server; then true; else " +
                                " echo \"Hillview not running on " + str(rh.host) +"\"; " +
                                " cat " + config.service_folder + "/hillview/nohup.out; false; fi")

def check_workers(config):
    """Checks all Hillview workers and aggregators"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_aggregators(lambda rh: check_worker(config, rh))
    config.run_on_all_workers(lambda rh: check_worker(config, rh))

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = ClusterConfiguration(args.config)
    check_webserver(config)
    check_workers(config)

if __name__ == "__main__":
    main()
