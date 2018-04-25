#!/usr/bin/env python3
# -*-python-*-

# This script runs a command on all worker hosts of a Hillview
# cluster.

from hillviewCommon import *
from optparse import OptionParser

def execute_command_on_all(config, command):
    print("Executing `", command, "' on", len(config.backends), "hosts")
    for host in config.backends:
        rh = RemoteHost(config.user, host)
        rh.run_remote_shell_command(command)

def main():
    parser = OptionParser(usage="%prog [options] configfile command")
    (options, args) = parser.parse_args()
    if len(args) < 2:
        usage(parser)
    config = load_config(parser, args[0])
    command = " ".join(args[1:])
    execute_command_on_all(config, command)

if __name__ == "__main__":
    main()
