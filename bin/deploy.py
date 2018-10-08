#!/usr/bin/env python3

"""This python program deploys the files needed by the Hillview service
   on the machines specified in the configuration file."""

# pylint: disable=invalid-name
from argparse import ArgumentParser
import tempfile
import os.path
from hillviewCommon import ClusterConfiguration, get_config

def prepare_webserver(config):
    """Deploys files needed by the Hillview web server"""
    print("Creating web service folder")
    assert isinstance(config, ClusterConfiguration)
    rh = config.get_webserver()
    print("Preparing web server", rh)
    rh.create_remote_folder(config.service_folder)
    rh.run_remote_shell_command("chown " + config.get_user() + " " + config.service_folder)
    rh.create_remote_folder(config.service_folder + "/hillview")

    major = config.tomcat_version[0:config.tomcat_version.find('.')]

    installTomcat = "cd " + config.service_folder + ";" + \
      "if [ ! -d " + config.tomcat + " ]; then " + \
      "wget http://archive.apache.org/dist/tomcat/tomcat-" + major + "/v" + \
      config.tomcat_version + "/bin/" + config.tomcat + ".tar.gz;" + \
      "tar xvfz " + config.tomcat + ".tar.gz;" + \
      "rm -f " + config.tomcat + ".tar.gz; fi"

    tomcatFolder = config.service_folder + "/" + config.tomcat
    rh.run_remote_shell_command(installTomcat)
    rh.run_remote_shell_command("rm -rf " + tomcatFolder + "/webapps/ROOT")
    rh.copy_file_to_remote(
        config.scriptFolder +
        "/../web/target/web-1.0-SNAPSHOT.war",
        tomcatFolder + "/webapps/ROOT.war", "")
    tmp = tempfile.NamedTemporaryFile(mode="w", delete=False)
    agg = config.get_aggregators()
    if agg:
        for a in agg:
            tmp.write(a.host + ":" + str(config.aggregator_port) + "\n")
    else:
        for h in config.get_workers():
            tmp.write(h.host + ":" + str(config.worker_port) + "\n")
    tmp.close()
    rh.copy_file_to_remote(tmp.name, config.service_folder + "/serverlist", "")
    os.unlink(tmp.name)
    if config.cleanup_on_install():
        rh.run_remote_shell_command(
            "cd " + config.service_folder + ";" + \
            "rm -f hillview-web.log hillview-web.log.* hillview-web.log*.lck")
    # link to web server logs
    rh.run_remote_shell_command("ln -sf " + config.service_folder + "/hillview-web.log " + \
                                config.service_folder + "/hillview/hillview-web.log")

def prepare_worker(config, rh):
    """Prepares files needed by a Hillview worker on a remote machine"""
    assert isinstance(config, ClusterConfiguration)
    print("Preparing worker", rh)
#    rh.run_remote_shell_command("sudo apt-get install libgfortran3")
    rh.create_remote_folder(config.service_folder)
    rh.run_remote_shell_command("chown " + config.get_user() + " " + config.service_folder)
    rh.create_remote_folder(config.service_folder + "/hillview")
    rh.copy_file_to_remote(
        config.scriptFolder +
        "/../platform/target/hillview-server-jar-with-dependencies.jar",
        config.service_folder + "/hillview", "")
    if config.cleanup_on_install():
        rh.run_remote_shell_command(
            "cd " + config.service_folder + "/hillview;"
            "rm -f hillview.log hillview.log.* hillview.log*.lck")

def prepare_aggregator(config, rh):
    """Prepares files needed by a Hillview aggregator on a remote machine"""
    assert isinstance(config, ClusterConfiguration)
    print("Preparing aggregator", rh)
    rh.create_remote_folder(config.service_folder)
    rh.run_remote_shell_command("chown " + config.get_user() + " " + config.service_folder)
    rh.create_remote_folder(config.service_folder + "/hillview")
    rh.copy_file_to_remote(
        config.scriptFolder +
        "/../platform/target/hillview-server-jar-with-dependencies.jar",
        config.service_folder + "/hillview", "")
    if config.cleanup_on_install():
        rh.run_remote_shell_command(
            "cd " + config.service_folder + "/hillview;"
            "rm -f hillview-agg.log hillview-agg.log.* hillview-agg.log*.lck")
    tmp = tempfile.NamedTemporaryFile(mode="w", delete=False)
    for h in rh.children:
        tmp.write(h + ":" + str(config.worker_port) + "\n")
    tmp.close()
    rh.copy_file_to_remote(tmp.name, config.service_folder + "/workers", "")
    os.unlink(tmp.name)
    rh.run_remote_shell_command("ln -sf " + config.service_folder + "/hillview-agg.log " + \
                                config.service_folder + "/hillview/hillview-agg.log")

def prepare_workers(config):
    """Prepares all Hillview workers"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_workers(lambda rh: prepare_worker(config, rh))

def prepare_aggregators(config):
    """Prepares all Hillview aggregators"""
    assert isinstance(config, ClusterConfiguration)
    config.run_on_all_aggregators(lambda rh: prepare_aggregator(config, rh))

def main():
    """Main function"""
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = get_config(parser, args)
    prepare_webserver(config)
    prepare_aggregators(config)
    prepare_workers(config)

if __name__ == "__main__":
    main()
