#!/usr/bin/env python3

# This script deletes a specific folder on all the machines in a Hillview cluster.
# pylint: disable=unused-wildcard-import,invalid-name,missing-docstring,wildcard-import,superfluous-parens,unused-variable
from optparse import OptionParser
from hillviewCommon import *

def delete_remote_folder(rh, folder):
    rh.run_remote_shell_command("rm -rf " + folder)

def delete_folder(config, folder):
    print("Deleting", folder, "from all hosts")
    run_on_all_backends(config, lambda rh: delete_remote_folder(rh, folder), True)

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
