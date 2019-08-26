"""Common functions used by the Hillview deployment scripts"""

# pylint: disable=invalid-name,too-few-public-methods, bare-except
import os.path
import os
import subprocess
import tempfile
import json
import getpass
from argparse import ArgumentParser
from hillviewConsoleLog import get_logger

logger = get_logger("hillviewCommon")

def execute_command(command):
    """Executes the specified command using a shell"""
    logger.info(command)
    exitcode = subprocess.call(command, shell=True)
    if exitcode != 0:
        error = "Exit code returned:" + str(exitcode)
        logger.error(error)
        exit(exitcode)

class RemoteHost:
    """Abstraction for a remote host"""
    def __init__(self, user, host, parent, heapsize="200M"):
        """Create a remote host"""
        assert isinstance(user, str)
        assert isinstance(host, str)
        assert parent is None or isinstance(parent, RemoteHost)
        self.host = host
        self.user = user
        self.parent = parent
        self.heapsize = heapsize
        self.isAggregator = False

    def uh(self):
        """user@host"""
        if self.user is not None:
            return self.user + "@" + self.host
        return self.host

    def run_remote_shell_command(self, command, verbose=True):
        """Executes a command on a remote machine"""
        file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        file.write(command)
        file.close()

        f = open(file.name)
        text = f.read()
        if verbose:
            message = "On " + str(self.host) + ": " + text
            logger.info(message)
        f.close()

        command = "ssh " + self.uh() + " bash -s < " + file.name
        exitcode = subprocess.call(command, shell=True)
        if exitcode != 0:
            error = "Exit code returned: " + str(exitcode)
            logger.error(error)
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

class RemoteAggregator(RemoteHost):
    """Abstraction for an aggregator"""
    def __init__(self, user, host, parent, children):
        "Create a remote aggregator"""
        super().__init__(user, host, parent)
        self.children = children
        self.isAggregator = True

class JsonConfig:
    """ Configuration read from a JSON file."""

    def __init__(self, d):
        """Create configuration from a dictionary"""
        for a, b in d.items():
            if isinstance(b, (list, tuple)):
                setattr(self, a, [JsonConfig(x) if isinstance(x, dict) else x for x in b])
            else:
                setattr(self, a, JsonConfig(b) if isinstance(b, dict) else b)

    def __getitem__(self, key):
        """Get a configuration field"""
        return self.__dict__[key]

    def has_key(self, k):
        """Check if configuration has a specific field"""
        return k in self.__dict__

    def __contains__(self, item):
        """Check if configuration has a specific field"""
        return item in self.__dict__

class ClusterConfiguration:
    """Represents a Hillview cluster"""

    def __init__(self, file):
        """Load the configuration file describing the Hillview deployment."""
        message = "Reading cluster configuration from " + file
        # Some default values, in case they are missing
        self.worker_port = 3569
        self.cleanup = False
        self.service_folder = "/home/hillview"
        self.default_heap_size = "5G"
        self.user = getpass.getuser()
        logger.info(message)
        if not os.path.exists(file):
            error = "Configuration file '" + file + "' does not exist"
            logger.info(error)
            exit(1)
        try:
            with open(file) as contents:
                stripped = "".join(line.partition("//")[0] for line in contents)
            self._jsonConfig = json.loads(stripped, object_hook=JsonConfig)
        except:
            error = "Error parsing configuration file " + file
            logger.error(error)
            exit(1)
        if not os.path.isabs(self._jsonConfig.service_folder):
            error = "service_folder must be an absolute path in configuration file " + \
                    self._jsonConfig.service_folder
            logger.error(error)
            exit(1)
        # The path where the current script is installed
        self.scriptFolder = os.path.dirname(os.path.abspath(__file__))
        self.service_folder = self._jsonConfig.service_folder
        self.worker_port = self._jsonConfig.worker_port
        self.tomcat = self._jsonConfig.tomcat
        self.tomcat_version = self._jsonConfig.tomcat_version
        if hasattr(self._jsonConfig, "user"):
            self.user = self._jsonConfig.user
        if hasattr(self._jsonConfig, "aggregator_port"):
            self.aggregator_port = self._jsonConfig.aggregator_port
        else:
            self.aggregator_port = 0

    def get_user(self):
        """Returns the user used by the hillview service"""
        return self.user

    def _get_heap_size(self, hostname):
        """The heap size used for the specified host"""
        if hasattr(self._jsonConfig, "workers_heapsize") and hostname in self._jsonConfig.workers_heapsize:
            return self._jsonConfig.workers_heapsize[hostname]
        return self._jsonConfig.default_heap_size

    def get_workers(self):
        """Returns an array of RemoteHost objects containing all workers"""
        webserver = self.get_webserver()
        if hasattr(self._jsonConfig, "aggregators"):
            return [RemoteHost(self.get_user(), h,
                               RemoteHost(self.get_user(), a.name, webserver),
                               self._get_heap_size(h))
                    for a in self._jsonConfig.aggregators
                    for h in a.workers]
        return [RemoteHost(self.get_user(), h, webserver, self._get_heap_size(h))
                for h in self._jsonConfig.workers]

    def get_webserver(self):
        """Returns a remote host representing the web server"""
        return RemoteHost(self.get_user(), self._jsonConfig.webserver, None)

    def get_aggregators(self):
        """Returns an array of RemoteAggregator objects"""
        if not hasattr(self._jsonConfig, "aggregators"):
            return []
        webserver = self.get_webserver()
        return [RemoteAggregator(self.get_user(), h.name, webserver, h.workers)
                for h in self._jsonConfig.aggregators]

    def cleanup_on_install(self):
        """Returns true if we need to cleaup when installing"""
        if hasattr(self._jsonConfig, "cleanup"):
            return self._jsonConfig.cleanup
        return False

    def run_on_all_aggregators(self, function):
        # pylint: disable=unused-argument
        """Run a lambda on all aggregators.  function is a lambda that takes a
        RemoteHost object as an argument."""
        for rh in self.get_aggregators():
            function(rh)

    def run_on_all_workers(self, function):
        # pylint: disable=unused-argument
        """Run a lambda on all workers.  function is a lambda that takes a
        RemoteHost object as an argument."""
        for rh in self.get_workers():
            function(rh)

def get_config(parser, args):
    """Given an argument parser and the results obtained by parsing,
       this function loads the cluster configuration.  This is always
       the config argument of the parser."""
    assert isinstance(parser, ArgumentParser)
    try:
        config = ClusterConfiguration(args.config)
        return config
    except Exception as e:
        print(str(e))
        parser.print_help()
        exit(1)
