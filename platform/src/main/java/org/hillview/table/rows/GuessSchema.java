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

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IStringColumn;
import org.hillview.utils.DateParsing;

import javax.annotation.Nullable;
import java.io.IOException;
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

        SchemaInfo(ContentsKind kind, boolean allowMissing) {
            this.kind = kind;
            this.allowMissing = allowMissing;
        }

        public String toString() {
            return this.kind.toString() + ", " +
                    (this.allowMissing ? "allow missing" : "no missing");
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
    private static final HashMap<ContentsKind, ContentsKind[]> successor =
            new HashMap<ContentsKind, ContentsKind[]>();

    static {
        successor.put(ContentsKind.None, new ContentsKind[]{
                ContentsKind.Integer, ContentsKind.Double,
                ContentsKind.Duration, ContentsKind.Date,
                ContentsKind.Json, ContentsKind.String
        });
        successor.put(ContentsKind.Integer,
                new ContentsKind[] { ContentsKind.Double, ContentsKind.Json,
                        ContentsKind.String
        });
        successor.put(ContentsKind.Double,
                new ContentsKind[] { ContentsKind.Json, ContentsKind.String });
        successor.put(ContentsKind.Date, new ContentsKind[]
                { ContentsKind.String });
        successor.put(ContentsKind.Duration, new ContentsKind[]
                {  ContentsKind.String });
        successor.put(ContentsKind.Json, new ContentsKind[] { ContentsKind.String });
    }

    @Nullable
    private
    DateParsing dateParser;

    public GuessSchema() {
        this.dateParser = null;
    }

    private void guess(@Nullable String value, SchemaInfo info) {
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
        throw new RuntimeException("Could not guess kind of `" + value + "' currently " + info);
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

    private static boolean isJsonValid(final String json) throws IOException {
        return isJsonValid(new StringReader(json));
    }

    private static boolean isJsonValid(final Reader reader)
            throws IOException {
        return isJsonValid(new JsonReader(reader));
    }

    /**
     * Throws if the value is not valid JSON
     * The gson parser is not strict enough: it parses
     * unquoted strings as JSON.String, so we have to do this manually.
     * Returns true if this is a complex json value, false otherwise.
     */
    private static boolean isJsonValid(final JsonReader jsonReader) throws IOException {
        JsonToken token;
        boolean isComplex = false;
        loop:
        while ((token = jsonReader.peek()) != JsonToken.END_DOCUMENT && token != null) {
            switch (token) {
                case BEGIN_ARRAY:
                    isComplex = true;
                    jsonReader.beginArray();
                    break;
                case END_ARRAY:
                    isComplex = true;
                    jsonReader.endArray();
                    break;
                case BEGIN_OBJECT:
                    isComplex = true;
                    jsonReader.beginObject();
                    break;
                case END_OBJECT:
                    isComplex = true;
                    jsonReader.endObject();
                    break;
                case NAME:
                    jsonReader.nextName();
                    break;
                case STRING:
                case NUMBER:
                case BOOLEAN:
                case NULL:
                    jsonReader.skipValue();
                    break;
                case END_DOCUMENT:
                    break loop;
                default:
                    throw new AssertionError(token);
            }
        }
        return isComplex;
    }

    private CanParse canParse(@Nullable String value, final ContentsKind with) {
        if (value == null)
            return CanParse.AsNull;
        switch (with) {
            case None:
                return CanParse.No;
            case String:
                return CanParse.Yes;
            case Date:
                try {
                    if (this.dateParser == null)
                        this.dateParser = new DateParsing(value);
                    this.dateParser.parse(value);
                    return CanParse.Yes;
                } catch (Exception ex) {
                    return CanParse.No;
                }
            case Integer:
                try {
                    // Allow empty strings as numbers
                    if (value.trim().isEmpty())
                        return CanParse.AsNull;
                    Integer.parseInt(value);
                    return CanParse.Yes;
                } catch (Exception ex) {
                    return CanParse.No;
                }
            case Json:
                return CanParse.No;
                // TODO: reinstate this
                /*
                try {
                    isJsonValid(value);
                    return CanParse.Yes;
                } catch (Exception ex) {
                    return CanParse.No;
                }
                */
            case Double:
                try {
                    // Allow empty strings as numbers
                    if (value.trim().isEmpty())
                        return CanParse.AsNull;
                    Double.parseDouble(value);
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
