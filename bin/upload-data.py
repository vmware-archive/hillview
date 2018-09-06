#!/usr/bin/env python3

# This script takes a set of files and a set of machines.
# It uploads the files to the given machines in round-robin fashion.
# The script can also be given an optional schema file.
# This file will be uploaded to all machines.
# The list of machines is provided in a Hillview configuration file.
# pylint: disable=unused-wildcard-import,invalid-name,missing-docstring,wildcard-import,superfluous-parens,unused-variable

from optparse import OptionParser
import os.path
from hillviewCommon import *

created_folders = set()

def create_remote_folder(remoteHost, folder):
    # pylint: disable=unnecessary-semicolon
    shortcut = "" + remoteHost.host + ":" + folder;
    if shortcut in created_folders:
        return
    remoteHost.create_remote_folder(folder)
    created_folders.add(shortcut)

def copy_file_to_remote_host(rh, source, folder, copyOption):
    create_remote_folder(rh, folder)
    rh.copy_file_to_remote(source, folder, copyOption)

def copy_schema(config, schema, folder, copyOption):
    # pylint: disable=line-too-long
    print("Copying", schema, "to all hosts")
    run_on_all_backends(config, lambda rh: copy_file_to_remote_host(rh, schema, folder, copyOption), True)

def copy_files(config, folder, filelist, copyOption):
    print("Copying", len(filelist), "files to all hosts")
    index = 0
    for f in filelist:
        host = config.backends[index]
        index = (index + 1) % len(config.backends)
        rh = RemoteHost(config.user, host)
        copy_file_to_remote_host(rh, f, folder, copyOption)

def main():
    # pylint: disable=line-too-long,unnecessary-semicolon
    parser = OptionParser(usage="%prog [options] config fileList\n" + \
                          "files in the list are uploaded in round-robin to the worker machines in the cluster")
    parser.add_option("-d", help="destination folder where output is written" +\
                      "  (if relative it is with respect to config.service_folder)",
                      dest="folder")
    parser.add_option("-L", help="Follow symlinks instead of ignoring them",
                      action="store_const", const="-L", dest="copyOption", default="");
    parser.add_option("-s", help="File that is loaded to all machines", dest="everywhere")
    (options, args) = parser.parse_args()
    if len(args) < 1:
        print("Not enough arguments supplied")
        usage(parser)
    config = load_config(parser, args[0])
    folder = options.folder
    if folder is None:
        print("Destination folder is mandatory")
        usage(parser)
    if not os.path.isabs(folder):
        folder = os.path.join(config.service_folder, folder)
    if options.everywhere != None:
        copy_schema(config, options.everywhere, folder, options.copyOption)
    copy_files(config, folder, args[1:], options.copyOption)
    print("Done.")

if __name__ == "__main__":
    main()
