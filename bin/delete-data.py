#!/usr/bin/env python
# We attempted to make this program work with both python2 and python3

"""This script deletes a specific folder on all the machines in a Hillview cluster."""
# pylint: disable=invalid-name

import os.path
from argparse import ArgumentParser
from hillviewCommon import ClusterConfiguration, get_config, get_logger

logger = get_logger("delete-data")

def delete_remote_folder(rh, folder):
    """Deletes folder on the remote host"""
    rh.run_remote_shell_command("if [ -d " + folder + " ]; then " +
                                "rm -rf " + folder + "; echo Deleted " +
                                "; else echo \"Directory " + str(folder) +
                                " doesn't exist.\"; fi")

def delete_folder(config, folder):
    """Delete a folder on all remote hosts"""
    assert isinstance(config, ClusterConfiguration)
    message = "Deleting " + folder + " from all hosts"
    logger.info(message)
    config.run_on_all_workers(lambda rh: delete_remote_folder(rh, folder))

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
    logger.info("Done")

if __name__ == "__main__":
    main()
