#!/usr/bin/env python
# We attempted to make this program work with both python2 and python3

"""This script takes a cluster configuration and a file pattern.
It downloads the files that match from all machines in the cluster."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
import os.path
import os
import errno
from hillviewCommon import execute_command, ClusterConfiguration, get_config, get_logger

logger = get_logger("download-data")

def copy_from_remote_host(rh, pattern):
    """Copy files matching the pattern from the remote machine"""
    try:
        os.mkdir(rh.host)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
    u = rh.user
    if u is None:
        u = ""
    else:
        u = rh.user + "@"
    command = "scp -r " + u + rh.host + ":" + pattern + " " + rh.host + " || true"
    message = "Copying the files from the remote machine " + str(rh.host)
    logger.info(message)
    execute_command(command)

def copy_files(config, pattern):
    """Copy files matching the specified pattern from all worker machines"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_workers(lambda rh: copy_from_remote_host(rh, pattern))

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    parser.add_argument("pattern", help="Filename pattern to download")
    args = parser.parse_args()
    config = get_config(parser, args)
    pattern = args.pattern
    copy_files(config, pattern)
    logger.info("Done.")

if __name__ == "__main__":
    main()
