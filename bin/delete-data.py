#!/usr/bin/env python3

# This script deletes a specific folder on all the machines in a Hillview cluster.

from hillviewCommon import *
from optparse import OptionParser

def delete_remote_folder(config, host, folder):
    rh = RemoteHost(config.user, host)
    rh.run_remote_shell_command("rm -rf " + folder)

def delete_folder(config, folder):
    print("Deleting", folder, "from all hosts")
    for w in config.backends:
        delete_remote_folder(config, w, folder)

def main():
    parser = OptionParser(usage="%prog [options] config folderToDelete")
    (options, args) = parser.parse_args()
    if len(args) != 2:
        print("Two arguments must be supplied")
        usage(parser)
    config = load_config(parser, args[0])
    folder = args[1]
    if not os.path.isabs(folder):
        folder = os.path.join(config.service_folder, folder)
    delete_folder(config, folder)

if __name__ == "__main__":
    main()
