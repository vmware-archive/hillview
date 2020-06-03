/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.sketches;

import org.hillview.table.api.IColumn;
import org.hillview.table.api.ISketchWorkspace;
import org.hillview.utils.JsonList;

/**
 * Workspace for the GroupBySketch.  It has one workspace for each bucket computed.
 * @param <SW>  Type of workspace used by the child sketch.
 */
public class GroupByWorkspace<SW> implements ISketchWorkspace {
    final IColumn column;
    final JsonList<SW> bucketWorkspace;   // one per bucket
    final SW missingWorkspace;

    GroupByWorkspace(IColumn column, JsonList<SW> bucketWorkspace, SW missingWorkspace) {
        this.column = column;
        this.bucketWorkspace = bucketWorkspace;
        this.missingWorkspace = missingWorkspace;
    }
}
