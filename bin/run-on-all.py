#!/usr/bin/env python3
# -*-python-*-

"""This script runs a command on all worker hosts of a Hillview cluster."""
# pylint: disable=invalid-name

from argparse import ArgumentParser, REMAINDER
from hillviewCommon import ClusterConfiguration, get_config, get_logger

logger = get_logger("run-on-all")

def execute_command_on_all(config, command):
    """Executes command on all workers"""
    assert isinstance(config, ClusterConfiguration)
    message = "Executing `" + str(command) + "' on " + str(len(config.get_workers())) + " hosts"
    logger.info(message)
    lam = lambda rh: rh.run_remote_shell_command(command)
    config.run_on_all_workers(lam)

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    parser.add_argument("command", help="command to run", nargs=REMAINDER)
    args = parser.parse_args()
    config = get_config(parser, args)
    command = " ".join(args.command)
    execute_command_on_all(config, command)

if __name__ == "__main__":
    main()
