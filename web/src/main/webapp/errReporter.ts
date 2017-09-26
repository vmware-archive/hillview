/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export interface ErrorReporter {
    reportError(message: string) : void;
    clear(): void;
}

export class ConsoleErrorReporter implements ErrorReporter {
    public static instance: ConsoleErrorReporter = new ConsoleErrorReporter();

    private constructor() {}

    public reportError(message: string) : void {
        console.log(message);
    }

    // We cannot clear the console
    public clear() : void {}
}