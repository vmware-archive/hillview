#!/bin/bash
#
# Copyright (c) 2020 VMware Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This script is used when connecting to a Greenplum database
# to dump data in an external web table.  See
# https://gpdb.docs.pivotal.io/6-10/admin_guide/load/topics/g-defining-a-command-based-writable-external-web-table.html
# and https://gpdb.docs.pivotal.io/6-10/ref_guide/sql_commands/CREATE_EXTERNAL_TABLE.html
# The script receives data at stdin

# Single argument is the directory where the data is to be dumped
DIR=$1
PREFIX="file"
mkdir -p ${DIR} || exit 1
echo "$(</dev/stdin)" >${DIR}/${PREFIX}${GP_SEGMENT_ID}