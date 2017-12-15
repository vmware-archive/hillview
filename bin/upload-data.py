#! /usr/bin/env python
# -*-python-*-

# This script takes a set of files and a set of machines.
# It uploads the files to the given machines in round-robin fashion.
# The script can also be given an optional schema file.
# This file will be uploaded to all machines.
# The list of machines is provided in an ansible inventory file in the section
# called "backends", e.g.:
# [backends]
# machine1
# machine2
# etc.
# Only the "backends" section is used to derive the list of machines.

from ansible.vars import VariableManager
from ansible.inventory import Inventory
from ansible.parsing.dataloader import DataLoader
from optparse import OptionParser
import getpass
import subprocess

def usage(parser):
    print parser.print_help()
    exit(1)

def parse_hosts(filename):
    variable_manager = VariableManager()
    loader = DataLoader()

    inventory = Inventory(
        loader = loader,
        variable_manager = variable_manager,
        host_list = filename
    )
    workers = inventory.get_hosts("backends")
    return workers

def execute_command(command):
    print command
    subprocess.call(command, shell=True)

def create_remote_folder(host, folder, user):
    if user is None:
        user = ""
    else:
        user = user + "@"
    command = "ssh " + user + host.name + " 'mkdir -p " + folder + "'"
    execute_command(command)

def copy_file_to_remote_host(source, host, folder, user):
    create_remote_folder(host, folder, user)
    if user is None:
        user = ""
    else:
        user = user + "@"
    command = "scp -C " + source + " " + user + host.name + ":" + folder + "/"
    execute_command(command)

def copy_schema(schema, folder, workers, user):
    print "Copying", schema, "to all hosts"
    for w in workers:
        copy_file_to_remote_host(schema, w, folder, user)

def copy_files(filelist, folder, workers, user):
    print "Copying", len(filelist), "files to all hosts"
    index = 0
    for f in filelist:
        host = workers[index]
        index = (index + 1) % len(workers)
        copy_file_to_remote_host(f, host, folder, user)

def main():
    parser = OptionParser(usage="%prog [options] fileList")
    parser.add_option("-i", help="List of machines to use", dest="hosts")
    parser.add_option("-u", help="Username", dest="user")
    parser.add_option("-d", help="destination folder where output is written",
                      dest="folder")
    parser.add_option("-s", help="optional JSON file describing the data schema", dest="schema")
    (options, args) = parser.parse_args()
    if options.hosts == None:
        usage(parser)
    workers = parse_hosts(options.hosts)
    user = options.user
    if user is None:
        user = getpass.getuser()
    if options.schema != None:
        copy_schema(options.schema, options.folder, workers, options.user)
    copy_files(args, options.folder, workers, options.user)

if __name__ == "__main__":
    main()
