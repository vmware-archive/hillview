#! /usr/bin/env python
# -*-python-*-

# This script takes a set of machines and deletes a specific folder on all the machines.
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

def delete_remote_folder(host, folder, user):
    if user is None:
        user = ""
    else:
        user = user + "@"
    command = "ssh " + user + host.name + " 'rm -rf " + folder + "'"
    execute_command(command)

def delete_folder(folder, workers, user):
    print "Deleting", folder, "from all hosts"
    for w in workers:
        delete_remote_folder(w, folder, user)

def main():
    parser = OptionParser(usage="%prog [options] folderToDelete")
    parser.add_option("-i", help="List of machines to use", dest="hosts")
    parser.add_option("-u", help="Username", dest="user")
    (options, args) = parser.parse_args()
    if options.hosts == None:
        print "Please specify hosts file"
        usage(parser)
    if len(args) != 1:
        usage(parser)
    folder = args[0]
    if folder.startswith("/"):
        print "Folder must be relative"
        usage(parser)
    workers = parse_hosts(options.hosts)
    user = options.user
    if user is None:
        user = getpass.getuser()
    delete_folder(folder, workers, options.user)

if __name__ == "__main__":
    main()
