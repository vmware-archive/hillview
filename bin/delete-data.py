#!/usr/bin/env python3

"""This script deletes a specific folder on all the machines in a Hillview cluster."""
# pylint: disable=invalid-name

import os.path
from argparse import ArgumentParser
from hillviewCommon import ClusterConfiguration, get_config

def delete_remote_folder(rh, folder):
    """Deletes folder on the remote host"""
    rh.run_remote_shell_command("rm -rf " + folder)

def delete_folder(config, folder):
    """Delete a folder on all remote hosts"""
    assert isinstance(config, ClusterConfiguration)
    print("Deleting", folder, "from all hosts")
    config.run_on_all_workers(lambda rh: delete_remote_folder(rh, folder), True)

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    parser.add_argument("folder", help="Folder to delete from all machines")
    args = parser.parse_args()
    config = get_config(parser, args)
    folder = args.folder
    if not os.path.isabs(folder):
        folder = os.path.join(config.service_folder, folder)
    delete_folder(config, folder)

if __name__ == "__main__":
    main()
