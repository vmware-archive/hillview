#!/usr/bin/env python3
# requires ansible and ansible-runner: pip3 install --user ansible ansible-runner
import os
from argparse import ArgumentParser
from configparser import ConfigParser
from tempfile import NamedTemporaryFile

import ansible_runner

from hillviewCommon import get_config

# sections in inventory
NAMENODE = "namenode"
DATANODE = "datanode"
DEFAULT_VARS = "all:vars"

# specifies which hadoop version to use
HADOOP_VERSION = "3.3.1"


def write_inventory_file(config, file):
    inventory = ConfigParser(allow_no_value=True)

    # use the webserver node as namenode
    inventory.add_section(NAMENODE)
    inventory.set(NAMENODE, config.get_webserver().host)

    # use the workers as datanodes
    inventory.add_section(DATANODE)
    for worker in config.get_workers():
        inventory.set(DATANODE, worker.host)

    inventory.add_section(DEFAULT_VARS)
    inventory.set(DEFAULT_VARS, "ansible_user", config.get_user())
    inventory.set(DEFAULT_VARS, "hadoop_version", HADOOP_VERSION)

    inventory.write(file)
    file.flush()


def get_deployment_dir():
    """
    Assumes there is a deployment folder in the project root that contains the needed ansible files.
    :return: The absolute path to the deployment folder.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(project_root, "deployment")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = get_config(parser, args)

    with NamedTemporaryFile(mode="w") as inventory_file:
        write_inventory_file(config, inventory_file)
        ansible_runner.run(
            project_dir=get_deployment_dir(),
            inventory=inventory_file.name,
            playbook="install-hdfs.yml"
        )
