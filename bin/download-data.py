#!/usr/bin/env python3

"""This script takes a file pattern.
It downloads the files that match from all machines in the cluster.
The list of machines is provided in a Hillview configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
import os.path
import os
import errno
from hillviewCommon import execute_command, ClusterConfiguration

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
    command = "scp -r " + u + rh.host + ":" + pattern + " " + rh.host
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
    config = ClusterConfiguration(args.config)
    pattern = args.pattern
    copy_files(config, pattern)
    print("Done.")

if __name__ == "__main__":
    main()
