#!/usr/bin/env python3

"""This script takes a set of files and a cluster configuration describing a set of machines.
   It uploads the files to the given machines in round-robin fashion.
   The script can also be given an optional schema file.
   This file will be uploaded to all machines.
   The list of machines is provided in a Hillview configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser, REMAINDER
import os.path
from hillviewCommon import ClusterConfiguration

created_folders = set()

def create_remote_folder(remoteHost, folder):
    """Creates a folder on a remote machine"""
    shortcut = "" + remoteHost.host + ":" + folder
    if shortcut in created_folders:
        return
    remoteHost.create_remote_folder(folder)
    created_folders.add(shortcut)

def copy_file_to_remote_host(rh, source, folder, copyOption):
    """Copy files in the specified folder to the remote machine"""
    create_remote_folder(rh, folder)
    rh.copy_file_to_remote(source, folder, copyOption)

def copy_everywhere(config, file, folder, copyOption):
    """Copy specified file to all worker machines"""
    assert isinstance(config, ClusterConfiguration)
    print("Copying", file, "to all hosts")
    config.run_on_all_workers(lambda rh: copy_file_to_remote_host(rh, file, folder, copyOption))

def copy_files(config, folder, filelist, copyOption):
    """Copy a set of files to all remote hosts"""
    assert isinstance(config, ClusterConfiguration)
    print("Copying", len(filelist), "files to all hosts")
    index = 0
    workers = config.get_workers()
    for f in filelist:
        rh = workers[index]
        index = (index + 1) % len(workers)
        copy_file_to_remote_host(rh, f, folder, copyOption)

def main():
    """Main function"""
    parser = ArgumentParser(epilog="The argument in the list are uploaded in round-robin " +
                            "to the worker machines in the cluster")
    parser.add_argument("config", help="json cluster configuration file")
    parser.add_argument("-d", "--directory",
                        help="destination folder where output is written" +\
                        "  (if relative it is with respect to config.service_folder)")
    parser.add_argument("-L", "--symlinks", help="Follow symlinks instead of ignoring them",
                        action="store_true")
    parser.add_argument("--common", "-s", help="File that is loaded to all machines")
    parser.add_argument("files", help="Files to copy", nargs=REMAINDER)
    args = parser.parse_args()
    config = ClusterConfiguration(args.config)
    folder = args.directory
    if folder is None:
        print("Directory argument is mandatory")
        parser.print_help()
        exit(1)
    if args.symlinks:
        copyOptions = "-L"
    else:
        copyOptions = ""
    if not os.path.isabs(folder):
        folder = os.path.join(config.service_folder, folder)
    if args.common != None:
        copy_everywhere(config, args.common, folder, copyOptions)
    copy_files(config, folder, args.files, copyOptions)
    print("Done.")

if __name__ == "__main__":
    main()
