# Common functions user by the Hillview deployment scripts

import os.path
from optparse import OptionParser
from importlib.machinery import SourceFileLoader
import subprocess
import tempfile

def usage(parser):
    assert isinstance(parser, OptionParser)
    print(parser.print_help())
    exit(1)

def load_config(parser, file):
    """Load the configuration file describing the Hillview deployment.
    This file defines a module which has a bunch of global variables.
    """
    print("Importing configuration from", file)
    (folder, basename) = os.path.split(file)
    if folder is None:
        folder = "."
    config = SourceFileLoader(basename, file).load_module()
    if not os.path.isabs(config.service_folder):
        print("service_folder must be an absolute path in configuration file",
              config.service_folder)
        exit(1)
    # The path where the current script is installed
    config.scriptFolder = os.path.dirname(os.path.abspath(__file__))
    return config

def execute_command(command):
    """Executes the specified command using a shell"""
    print(command)
    exitcode = subprocess.call(command, shell=True)
    if exitcode != 0:
        print("Exit code returned:", exitcode)
        exit(exitcode)

def run_on_all_backends(config, function):
    """Run a lambda on all back-ends.  function is a lambda that takes a
    RemoteHost object as an argument"""
    for h in config.backends:
        rh = RemoteHost(config.user, h)
        function(rh)

class RemoteHost:
    """Abstraction for a remote host"""
    def __init__(self, user, host):
        assert isinstance(user, str)
        assert isinstance(host, str)
        self.host = host
        self.user = user

    def uh(self):
        """user@host"""
        if self.user is not None:
            return self.user + "@" + self.host
        else:
            return self.host

    def run_remote_shell_command(self, command):
        """Executes a command on a remote machine"""
        file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        file.write(command)
        file.close()

        f = open(file.name)
        text = f.read()
        print("On", self.host, ":", text)
        f.close()

        command = "ssh " + self.uh() + " bash -s < " + file.name
        exitcode = subprocess.call(command, shell=True)
        if exitcode != 0:
            print("Exit code returned:", exitcode)
            exit(exitcode)
        os.unlink(file.name)

    def create_remote_folder(self, folder):
        """Ensures that a folder exists on the remote host"""
        self.run_remote_shell_command("mkdir -p " + folder)

    def copy_file_to_remote(self, source, dest, copyOption):
        """Copy a file to the remote host as the specified destination if not changed.
           copyOption can be -L to follow symlinks."""
        u = self.user
        if u is None:
            user = ""
        else:
            u = self.user + "@"
        command = "rsync -u " + copyOption + " " + source + " " + u + self.host + ":" + dest
        execute_command(command)

    def __str__(self):
        return self.host
