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

package org.hillview.utils;

/**
 * Core API for logging in hillview.
 * The hillview logging keeps each log message on a single line.
 * It uses commas to separate the fields.
 * The result produced by format and arguments is escaped: quotes and newlines are escaped.
 */
interface IHillviewLogger {
    void info(String message, String format, Object... arguments);
    void warn(String message, String format, Object... arguments);
    void debug(String message, String format, Object... arguments);
    void error(String message, Throwable ex);
    void error(String message, String format, Object... arguments);

    default void info(String message) { this.info(message, ""); }
    default void warn(String message) { this.warn(message, ""); }
    default void debug(String message) { this.debug(message, ""); }
    default void error(String message) { this.error(message, ""); }
}
