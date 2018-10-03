#!/usr/bin/env python3
# -*-python-*-

"""This script runs a command on all worker hosts of a Hillview cluster."""
# pylint: disable=invalid-name

from argparse import ArgumentParser, REMAINDER
from hillviewCommon import ClusterConfiguration, get_config

def execute_command_on_all(config, command, parallel):
    """Executes command on all workers"""
    assert isinstance(config, ClusterConfiguration)
    print("Executing `", command, "' on", len(config.get_workers()), "hosts")
    lam = lambda rh: rh.run_remote_shell_command(command)
    config.run_on_all_workers(lam, parallel)

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    parser.add_argument("command", help="command to run", nargs=REMAINDER)
    args = parser.parse_args()
    config = get_config(parser, args)
    command = " ".join(args.command)
    execute_command_on_all(config, command, False)

if __name__ == "__main__":
    main()
