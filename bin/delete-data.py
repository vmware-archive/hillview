#!/usr/bin/env python3

"""This script deletes a specific folder on all the machines in a Hillview cluster."""
# pylint: disable=invalid-name

import os.path
from argparse import ArgumentParser
from hillviewCommon import run_on_all_backends, load_config

def delete_remote_folder(rh, folder):
    """Deletes folder on the remote host"""
    rh.run_remote_shell_command("rm -rf " + folder)

def delete_folder(config, folder):
    """Delete a folder on all remote hosts"""
    print("Deleting", folder, "from all hosts")
    run_on_all_backends(config, lambda rh: delete_remote_folder(rh, folder), True)

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    parser.add_argument("folder", help="Folder to delete from all machines")
    args = parser.parse_args()
    config = load_config(args.config)
    folder = config.folder
    if not os.path.isabs(folder):
        folder = os.path.join(config.service_folder, config.folder)
    delete_folder(config, folder)

if __name__ == "__main__":
    main()
