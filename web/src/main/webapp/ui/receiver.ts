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


// Receivers are intermediate classes that subscribe to a stream of
// updates and update a view on each update.

import {FullPage, PageTitle} from "./fullPage";
import {TableTargetAPI} from "../modules";
import {SchemaClass} from "../schemaClass";
import {PartialResult} from "../util";
import {Receiver, RpcRequest} from "../rpc";
import {ChartOptions} from "./ui";

/**
 * Most views use all these arguments.
 */
export interface CommonArgs {
    /**
     * Title for the produced page.
     */
    title: PageTitle,
    /**
     * The initiator of the request.
     */
    remoteObject: TableTargetAPI,
    /**
     * Number of rows in source dataset.
     */
    rowCount: number,
    /**
     * Schema of the originator.
     */
    schema: SchemaClass,
}

/**
 * Arguments passed to most receivers.
 */
export interface ReceiverCommonArgs extends CommonArgs {
    /**
     * The page that started the request.
     */
    originalPage: FullPage,
    /**
     * Options for the resulting chart.
     */
    options: ChartOptions
}

export abstract class ReceiverCommon<T> extends Receiver<T> {
    protected constructor(protected args: ReceiverCommonArgs, operation: RpcRequest<PartialResult<T>>, description: string) {
        super(args.options.reusePage ? args.originalPage : args.originalPage.dataset.newPage(args.title, args.originalPage),
            operation, description)
    }
}