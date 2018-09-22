#!/usr/bin/env python3

"""This program checks if the Hillview service is running on all machines
   specified in the configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import RemoteHost, run_on_all_backends, load_config

def check_webserver(config):
    """Checks if the Hillview web server is running"""
    rh = RemoteHost(config.user, config.webserver)
    rh.run_remote_shell_command("if pgrep -f tomcat; then true; else " +
                                " echo \"Web server not running on " + str(rh.host) +"\"; " +
                                " false; fi")

def check_backend(config, rh):
    """Checks if the Hillview service is running on a remote machine"""
    rh.run_remote_shell_command("if pgrep -f hillview-server; then true; else " +
                                " echo \"Hillview not running on " + str(rh.host) +"\"; " +
                                " cat " + config.service_folder + "/hillview/nohup.out; false; fi")

def check_backends(config):
    """Checks all Hillview backend workers"""
    run_on_all_backends(config, lambda rh: check_backend(config, rh), True)

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = load_config(args.config)
    check_webserver(config)
    check_backends(config)

if __name__ == "__main__":
    main()
