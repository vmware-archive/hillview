#!/usr/bin/env python3

"""This Python program stops the Hillview service on the machines specified in the
   configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import ClusterConfiguration

def stop_webserver(config):
    """Stops the Hillview web server"""
    assert isinstance(config, ClusterConfiguration)
    rh = config.get_webserver()
    print("Stopping web server on", rh)
    rh.run_remote_shell_command("if pgrep -f tomcat; then " + config.service_folder + "/" +
                                config.tomcat + "/bin/shutdown.sh; echo Stopped; else " +
                                " echo \"Web server already stopped on " + str(rh.host) +"\"; " +
                                " true; fi")
    rh.run_remote_shell_command(
        # The true is there to ignore the exit code of pkill
        "pkill -f Bootstrap; true")

def stop_worker(rh):
    """Stops a Hillview service on a remote worker machine"""
    print("Stopping hillview on ", rh.host)
    rh.run_remote_shell_command("if pgrep -f hillview-server; then pkill -f hillview-server; true; " +
                                "echo Stopped ; else echo \"Hillview already stopped on " +
                                str(rh.host) +"\"; true; fi")
def stop_backends(config):
    """Stops all Hillview workers"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_workers(stop_worker, True)
    config.run_on_all_aggregators(stop_worker, True)

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = ClusterConfiguration(args.config)
    stop_webserver(config)
    stop_backends(config)

if __name__ == "__main__":
    main()
