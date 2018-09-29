"""Common functions user by the Hillview deployment scripts"""

# pylint: disable=invalid-name,too-few-public-methods
import os.path
import subprocess
import tempfile
import json

def execute_command(command):
    """Executes the specified command using a shell"""
    print(command)
    exitcode = subprocess.call(command, shell=True)
    if exitcode != 0:
        print("Exit code returned:", exitcode)
        exit(exitcode)

class RemoteHost(object):
    """Abstraction for a remote host"""
    def __init__(self, user, host, heapsize="2G"):
        """Create a remote host"""
        assert isinstance(user, str)
        assert isinstance(host, str)
        self.host = host
        self.user = user
        self.heapsize = heapsize

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

class RemoteAggregator(RemoteHost):
    """Abstraction for an aggregator"""
    def __init__(self, user, host, children):
        "Create a remote aggregator"""
        super().__init__(user, host)
        self.children = children

class JsonConfig(object):
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

class ClusterConfiguration(object):
    """Represents a Hillview cluster"""

    def __init__(self, file):
        """Load the configuration file describing the Hillview deployment."""
        print("Importing configuration from", file)
        with open(file) as contents:
            stripped = "".join(line.partition("//")[0] for line in contents)
        self.jsonConfig = json.loads(stripped, object_hook=JsonConfig)
        if not os.path.isabs(self.jsonConfig.service_folder):
            print("service_folder must be an absolute path in configuration file",
                  self.jsonConfig.service_folder)
            exit(1)
        # The path where the current script is installed
        self.scriptFolder = os.path.dirname(os.path.abspath(__file__))
        self.service_folder = self.jsonConfig.service_folder
        self.worker_port = self.jsonConfig.worker_port
        self.aggregator_port = self.jsonConfig.aggregator_port
        self.tomcat = self.jsonConfig.tomcat
        self.tomcat_version = self.jsonConfig.tomcat_version

    def get_user(self):
        return self.jsonConfig.user

    def _get_heap_size(self, hostname):
        if hostname in self.jsonConfig.workers_heapsize:
            return self.jsonConfig.workers_heapsize[hostname]
        return self.jsonConfig.default_heap_size

    def get_workers(self):
        """Returns an array of RemoteHost objects containing all workers"""
        if hasattr(self.jsonConfig, "aggregators"):
            return [RemoteHost(self.jsonConfig.user, h, self._get_heap_size(h))
                    for a in self.jsonConfig.aggregators
                    for h in a.workers]
        return [RemoteHost(self.jsonConfig.user, h, self._get_heap_size(h))
                for h in self.jsonConfig.workers]

    def get_webserver(self):
        """Returns a remote host representing the web server"""
        return RemoteHost(self.jsonConfig.user, self.jsonConfig.webserver)

    def get_aggregators(self):
        """Returns an array of RemoteAggregator objects"""
        if not hasattr(self.jsonConfig, "aggregators"):
            return []
        return [RemoteAggregator(self.jsonConfig.user, h.name, h.workers)
                for h in self.jsonConfig.aggregators]

    def cleanup_on_install(self):
        """Returns true if we need to cleaup when installing"""
        return self.jsonConfig.cleanup

    def run_on_all_aggregators(self, function, parallel=True):
        # pylint: disable=unused-argument
        """Run a lambda on all aggregators.  function is a lambda that takes a
        RemoteHost object as an argument.  If parallel is True the function is
        run concurrently."""
        for rh in self.get_aggregators():
            function(rh)

    def run_on_all_workers(self, function, parallel=True):
        # pylint: disable=unused-argument
        """Run a lambda on all workers.  function is a lambda that takes a
        RemoteHost object as an argument.  If parallel is True the function is
        run concurrently."""
        # Unfortunately there seems to be no way to reliably
        # run something in parallel in Python, so this is not working yet.
        for rh in self.get_workers():
            function(rh)
