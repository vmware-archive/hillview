#!/usr/bin/env python
# This script installs Hillview next to a greenplum database.
# It needs a config-greenplum.json file that has a description of the
# Greenplum database installation.  See also the section
# on Greenplum installation from https://github.com/vmware/hillview/blob/master/docs/userManual.md

#  Copyright (c) 2020 VMware Inc. All Rights Reserved.
#  SPDX-License-Identifier: Apache-2.0
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# pylint: disable=invalid-name
from argparse import ArgumentParser
from jproperties import Properties
import os
import tempfile
from hillviewCommon import ClusterConfiguration, get_config, get_logger, execute_command

def main():
    parser = ArgumentParser()
    parser.add_argument("config", help="json cluster configuration file")
    args = parser.parse_args()
    config = get_config(parser, args)

    execute_command("./package-binaries.sh")
    web = config.get_webserver()
    web.copy_file_to_remote("../hillview-bin.zip", ".", "")
    web.run_remote_shell_command("unzip -o hillview-bin.zip")
    web.copy_file_to_remote("config-greenplum.json", "bin", "")
    web.run_remote_shell_command("cd bin; ./upload-data.py -d . -s move-greenplum.sh config-greenplum.json")
    web.run_remote_shell_command("cd bin; ./redeploy.sh -s config-greenplum.json")
    web.copy_file_to_remote("../repository/PROGRESS_DATADIRECT_JDBC_DRIVER_PIVOTAL_GREENPLUM_5.1.4.000275.jar",
                             config.service_folder + "/" + config.tomcat + "/lib", "")
    # Generate properties file
    with open("greenplum.properties", "rb") as f:
        p = Properties()
        p.load(f, "utf-8")
        p["greenplumMoveScript"] = config.service_folder + "/move-greenplum.sh"
        p["hideDemoMenu"] = "true"
        p["enableSaveAs"] = "true"
    tmp = tempfile.NamedTemporaryFile(mode="w", delete=False)
    p.store(tmp, encoding="utf-8")
    tmp.close()
    web.copy_file_to_remote(tmp.name, config.service_folder + "/hillview.properties", "")
    os.remove(tmp.name)

if __name__ == "__main__":
    main()
