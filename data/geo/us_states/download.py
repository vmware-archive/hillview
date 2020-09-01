#!/usr/bin/env python3

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

# Downloads the US states map
# pylint: disable=invalid-name,missing-docstring

import os
import subprocess

def execute_command(command):
    """Executes the specified command using a shell"""
    print(command)
    exitcode = subprocess.call(command, shell=True)
    if exitcode != 0:
        print("Exit code returned:", exitcode)
        exit(exitcode)

def main():
    site = "https://www2.census.gov/geo/tiger/GENZ2019/shp/"
    file = "cb_2019_us_state_20m.zip"
    execute_command("wget -q " + site + file)
    execute_command("unzip -o " + file)
    os.unlink(file)

if __name__ == "__main__":
    main()
