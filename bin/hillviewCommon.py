"""Common functions user by the Hillview deployment scripts"""

# pylint: disable=invalid-name,too-few-public-methods
import os.path
import subprocess
import tempfile
import json

class Config(object):
    """ Configuration class.  The fields are populated dynamically
    from a JSON configuration file."""

    def __init__(self, d):
        """Create configuration from a dictionary"""
        for a, b in d.items():
            if isinstance(b, (list, tuple)):
                setattr(self, a, [Config(x) if isinstance(x, dict) else x for x in b])
            else:
                setattr(self, a, Config(b) if isinstance(b, dict) else b)

    def __getitem__(self, key):
        """Get a configuration field"""
        return self.__dict__[key]

    def has_key(self, k):
        """Check if configuration has a specific field"""
        return k in self.__dict__

    def __contains__(self, item):
        """Check if configuration has a specific field"""
        return item in self.__dict__


def load_config(file):
    """Load the configuration file describing the Hillview deployment.
    """
    print("Importing configuration from", file)
    with open(file) as contents:
        stripped = "".join(line.partition("//")[0] for line in contents)
    config = json.loads(stripped, object_hook=Config)
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

def run_on_all_backends(config, function, parallel):
    """Run a lambda on all back-ends.  function is a lambda that takes a
    RemoteHost object as an argument.  If parallel is True the function is
    run concurrently."""
    # Unfortunately there seems to be no way to reliably
    # run something in parallel in Python, so this is not working yet.
    # pylint: disable=unused-argument
    parallel = False
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
            u = ""
        else:
            u = self.user + "@"
        command = "rsync -u " + copyOption + " " + source + " " + u + self.host + ":" + dest
        execute_command(command)

    def __str__(self):
        """Converts host to a string"""
        return self.host
