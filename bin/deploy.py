#!/usr/bin/env python3

# This python program deploys the files needed by the Hillview service
# on the machines specified in the configuration file.

from hillviewCommon import *
from optparse import OptionParser
import tempfile

def prepare_webserver(config):
    """Deploys files needed by the Hillview web server"""
    print("Preparing web server", config.webserver)
    print("Creating web service folder")
    rh = RemoteHost(config.user, config.webserver)
    rh.create_remote_folder(config.service_folder)
    rh.run_remote_shell_command("chown " + config.user + " " + config.service_folder)
    rh.create_remote_folder(config.service_folder + "/hillview")

    major=config.tomcat_version[0:config.tomcat_version.find('.')-1]

    installTomcat="cd " + config.service_folder + ";" + \
      "if [ ! -d " + config.tomcat + " ]; then " + \
      "wget http://archive.apache.org/dist/tomcat/tomcat-" + major + "/v" + \
      config.tomcat_version + "/bin/" + config.tomcat + ".tar.gz;" + \
      "tar xvfz " + config.tomcat + ".tar.gz;" + \
      "rm -f " + config.tomcat + ".tar.gz; fi"

    tomcatFolder = config.service_folder + "/" + config.tomcat
    rh.run_remote_shell_command(installTomcat)
    rh.run_remote_shell_command("rm -rf " + tomcatFolder + "/webapps/ROOT")
    rh.copy_file_to_remote(
        "../web/target/web-1.0-SNAPSHOT.war",
        tomcatFolder + "/webapps/ROOT.war")
    tmp = tempfile.NamedTemporaryFile(mode="w", delete=False)
    for h in config.backends:
        tmp.write(h + ":" + str(config.backend_port) + "\n")
    tmp.close()
    rh.copy_file_to_remote(tmp.name, config.service_folder + "/serverlist")
    os.unlink(tmp.name)
    if config.cleanup:
        rh.run_remote_shell_command(
            "cd " + config.service_folder + ";" + \
            "rm -f hillview-web.log hillview-web.log.* hillview-web.log*.lck")
    # link to web server logs
    rh.run_remote_shell_command("ln -sf " + config.service_folder + "/hillview-web.log " + \
                                config.service_folder + "/hillview/hillview-web.log")

def prepare_backend(config, rh):
    """Prepares files needed by a Hillview service on a remote machine"""
    print("Preparing backend", rh)
#    rh.run_remote_shell_command("sudo apt-get install libgfortran3")
    rh.create_remote_folder(config.service_folder)
    rh.run_remote_shell_command("chown " + config.user + " " + config.service_folder)
    rh.create_remote_folder(config.service_folder + "/hillview")
    rh.copy_file_to_remote(
        "../platform/target/hillview-server-jar-with-dependencies.jar", config.service_folder + "/hillview")
    if config.cleanup:
        rh.run_remote_shell_command(
            "cd " + config.service_folder + "/hillview;"
            "rm -f hillview.log hillview.log.* hillview.log*.lck")

def prepare_backends(config):
    """Prepares all Hillview backend workers"""
    run_on_all_backends(config, lambda rh: prepare_backend(config, rh))

def main():
    parser = OptionParser(usage="%prog config_file")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        usage(parser)
    config = load_config(parser, args[0])
    prepare_webserver(config)
    prepare_backends(config)

if __name__ == "__main__":
    main()
