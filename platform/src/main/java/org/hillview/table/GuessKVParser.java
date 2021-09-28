/*
 * Copyright (c) 2021 VMware Inc. All Rights Reserved.
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

package org.hillview.table;

import org.hillview.table.api.ContentsKind;
import org.hillview.utils.IKVParsing;
import org.hillview.utils.JsonFieldsExtractor;
import org.hillview.utils.KVParsing;

public class GuessKVParser {
    /**
     * Guess which parser to use for extracting data from a specific column.
     * @param col  Column to parse as KV-pairs.
     */
    public static IKVParsing getParserForColumn(ColumnDescription col) {
        IKVParsing parsing;
        if (col.kind == ContentsKind.Json) {
            parsing = new JsonFieldsExtractor();
        } else if (col.kind == ContentsKind.String) {
            if (col.name.equals("StructuredData"))
                parsing = KVParsing.createRFC5424StructuredDataParser();
            else
                parsing = KVParsing.createHttpHeaderParser();
        } else {
            throw new RuntimeException("Unexpected column type: " + col.kind);
        }
        return parsing;
    }
}
