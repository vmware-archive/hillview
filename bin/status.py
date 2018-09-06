#!/usr/bin/env python3

# This python checks if the Hillview service is running on all machines
# specified in the configuration file.
# pylint: disable=unused-wildcard-import,invalid-name,missing-docstring,wildcard-import,superfluous-parens,unused-variable

from optparse import OptionParser
from hillviewCommon import *

def check_webserver(config):
    """Checks if the Hillview web server is running"""
    rh = RemoteHost(config.user, config.webserver)
    rh.run_remote_shell_command("if pgrep -f tomcat; then true; else " +
                                " echo \"Web server not running on " + str(rh.host) +"\"; " +
                                " false; fi")

def check_backend(config, rh):
    """Check if the Hillview service is running on a remote machine"""
    rh.run_remote_shell_command("if pgrep -f hillview-server; then true; else " +
                                " echo \"Hillview not running on " + str(rh.host) +"\"; " +
                                " cat " + config.service_folder + "/hillview/nohup.out; false; fi")

def check_backends(config):
    """Starts all Hillview backend workers"""
    run_on_all_backends(config, lambda rh: check_backend(config, rh), True)

def main():
    parser = OptionParser(usage="%prog config_file")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        usage(parser)
    config = load_config(parser, args[0])
    check_webserver(config)
    check_backends(config)

if __name__ == "__main__":
    main()
