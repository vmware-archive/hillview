/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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

package org.hillview.table.rows;

import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IStringColumn;
import org.hillview.utils.DateParsing;

import javax.annotation.Nullable;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;

/**
 * Helper class to guess the schema of data given a set of strings.
 */
public class GuessSchema {
    public static class SchemaInfo {
        public ContentsKind kind;
        public boolean      allowMissing;

        public SchemaInfo(ContentsKind kind, boolean allowMissing) {
            this.kind = kind;
            this.allowMissing = allowMissing;
        }

        public SchemaInfo() {
            this(ContentsKind.None, false);
        }
    }

    enum CanParse {
        Yes,
        No,
        AsNull
    }

    /**
     * A lattice of contents kind.  Things can move only up in the lattice.
     */
    static final HashMap<ContentsKind, ContentsKind[]> successor =
            new HashMap<ContentsKind, ContentsKind[]>();

    static {
        successor.put(ContentsKind.None, new ContentsKind[]{
                ContentsKind.Integer, ContentsKind.Double,
                ContentsKind.Duration, ContentsKind.Date, ContentsKind.Json, ContentsKind.String });
        successor.put(ContentsKind.Integer,
                new ContentsKind[] { ContentsKind.Double, ContentsKind.Json, ContentsKind.String });
        successor.put(ContentsKind.Double,
                new ContentsKind[] { ContentsKind.Json, ContentsKind.String });
        successor.put(ContentsKind.Date, new ContentsKind[] { ContentsKind.String });
        successor.put(ContentsKind.Duration, new ContentsKind[] { ContentsKind.String });
        successor.put(ContentsKind.Json, new ContentsKind[] { ContentsKind.String });
    }

    @Nullable
    DateParsing parser;

    public GuessSchema() {
        this.parser = null;
    }

    void guess(@Nullable String value, SchemaInfo info) {
        if (info.kind == ContentsKind.String)
            return;
        CanParse cp = this.canParse(value, info.kind);
        if (cp == CanParse.Yes)
            return;
        if (cp == CanParse.AsNull) {
            info.allowMissing = true;
            return;
        }
        ContentsKind[] succ = successor.get(info.kind);
        for (ContentsKind c: succ) {
            cp = this.canParse(value, c);
            if (cp == CanParse.AsNull) {
                info.kind = c;
                info.allowMissing = true;
                return;
            } else if (cp == CanParse.Yes) {
                info.kind = c;
                return;
            }
        }
        throw new RuntimeException("Could not guess kind of " + value);
    }

    public SchemaInfo guess(Iterable<String> values) {
        SchemaInfo current = new SchemaInfo(ContentsKind.None, false);
        for (String s: values) {
            this.guess(s, current);
            if (current.kind == ContentsKind.String)
                return current;
        }
        return current;
    }

    public SchemaInfo guess(IStringColumn column) {
        SchemaInfo current = new SchemaInfo(ContentsKind.None, false);
        for (int i=0; i < column.sizeInRows(); i++) {
            this.guess(column.getString(i), current);
            if (current.kind == ContentsKind.String)
                return current;
        }
        return current;
    }

    CanParse canParse(@Nullable String value, final ContentsKind with) {
        if (value == null)
            return CanParse.AsNull;
        switch (with) {
            case None:
                return CanParse.No;
            case Category:
            case String:
                return CanParse.Yes;
            case Date:
                try {
                    if (this.parser == null)
                        this.parser = new DateParsing(value);
                    this.parser.parse(value);
                    return CanParse.Yes;
                } catch (Exception ex) {
                    return CanParse.No;
                }
            case Integer:
                try {
                    // Allow empty strings as numbers
                    if (value.trim().isEmpty())
                        return CanParse.AsNull;
                    int ignored = Integer.parseInt(value);
                    return CanParse.Yes;
                } catch (Exception ex) {
                    return CanParse.No;
                }
            case Json:
                try {
                    Reader reader = new StringReader(value);
                    JsonReader jReader = new JsonReader(reader);
                    Streams.parse(jReader);
                    return CanParse.Yes;
                } catch (Exception ex) {
                    return CanParse.No;
                }
            case Double:
                try {
                    // Allow empty strings as numbers
                    if (value.trim().isEmpty())
                        return CanParse.AsNull;
                    double ignored = Double.parseDouble(value);
                    return CanParse.Yes;
                } catch (Exception ex) {
                    return CanParse.No;
                }
            case Duration:
                // TODO: how do durations look as strings?
                return CanParse.No;
        }
        throw new RuntimeException("Unexpected kind " + with);
    }
}
