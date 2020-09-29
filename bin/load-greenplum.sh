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
# to load data from an external web table.  See
# https://gpdb.docs.pivotal.io/6-10/admin_guide/load/topics/g-defining-a-command-based-writable-external-web-table.html
# and https://gpdb.docs.pivotal.io/6-10/ref_guide/sql_commands/CREATE_EXTERNAL_TABLE.html

# Single argument is the directory where the data to load is located
# This script is invoked by each segment in a segment host
DIR=$1
REGEX=".*file([0-9]+).*"
for file in ${DIR}/*.db; do
   if [[ ${file} =~ ${REGEX} ]]; then
      NUMBER=${BASH_REMATCH[1]}
      # Only allocate some files to each segment
      SEGMENT=$(( ${NUMBER} % ${GP_SEGMENT_COUNT} ))
      if [[ "${GP_SEGMENT_ID}" == "${SEGMENT}" ]]; then
          cat ${file}
          rm ${file}
      fi
   else
       echo "Unexpected file name"
       exit 1
   fi
done
