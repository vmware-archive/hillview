#!/usr/bin/env python3

"""This python starts the Hillview service on the machines
   specified in the configuration file."""
# pylint: disable=invalid-name

from argparse import ArgumentParser
from hillviewCommon import RemoteHost, RemoteAggregator, ClusterConfiguration

def start_webserver(config):
    """Starts the Hillview web server"""
    assert isinstance(config, ClusterConfiguration)
    rh = config.get_webserver()
    print("Starting web server", rh)
    rh.run_remote_shell_command(
        "export WEB_CLUSTER_DESCRIPTOR=serverlist; cd " + config.service_folder + "; nohup " + \
        config.tomcat + "/bin/startup.sh &")

def start_worker(config, rh):
    """Starts the Hillview worker on a remote machine"""
    assert isinstance(rh, RemoteHost)
    assert isinstance(config, ClusterConfiguration)
    print("Starting worker", rh)
    gclog = config.service_folder + "/hillview/gc.log"
    rh.run_remote_shell_command(
        "cd " + config.service_folder + "/hillview; " + \
        "nohup java -Dlog4j.configurationFile=./log4j.properties -server -Xms" + rh.heapsize + \
        " -Xmx" + rh.heapsize + " -Xloggc:" + gclog + \
        " -jar " + config.service_folder + \
        "/hillview/hillview-server-jar-with-dependencies.jar 0.0.0.0:" + \
        str(config.worker_port) + " >nohup.out 2>&1 &")
    # Check to see whether the remote service is still running.  Sometimes it fails right away
    rh.run_remote_shell_command("if pgrep -f hillview-server; then echo Started; else " +
                                " echo \"Could not start hillview worker on " + str(rh.host) +"\"; " +
                                " cat " + config.service_folder + "/hillview/nohup.out; false; fi")

def start_aggregator(config, agg):
    """Starts a Hillview aggregator"""
    assert isinstance(agg, RemoteAggregator)
    assert isinstance(config, ClusterConfiguration)
    print("Starting aggregator", agg)
    agg.run_remote_shell_command(
        "cd " + config.service_folder + "/hillview; " + \
        "nohup java -Dlog4j.configurationFile=./log4j.properties -server " + \
        " -jar " + config.service_folder + \
        "/hillview/hillview-server-jar-with-dependencies.jar " + \
        config.service_folder + "/workers 0.0.0.0:" + \
        str(config.aggregator_port) + " >nohup.agg 2>&1 &")
    # Check to see whether the remote service is still running.  Sometimes it fails right away
    agg.run_remote_shell_command("if pgrep -f hillview-server; then echo Started; else " +
                                 " echo \"Could not start hillview aggregator on " + str(agg.host) +"\"; " +
                                 " cat " + config.service_folder + "/hillview/nohup.agg; false; fi")

def start_aggregators(config):
    """Starts all Hillview aggregators"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_aggregators(lambda rh: start_aggregator(config, rh))

def start_workers(config):
    """Starts all Hillview workers"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_workers(lambda rh: start_worker(config, rh))

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = ClusterConfiguration(args.config)
    start_webserver(config)
    start_workers(config)
    start_aggregators(config)

if __name__ == "__main__":
    main()
