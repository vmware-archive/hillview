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
 *
 */

package org.hiero.remoting;

import akka.actor.ActorRef;
import org.hiero.dataset.api.IDataSet;

import java.io.Serializable;

/**
 * Wraps the necessary references in order to execute a zip between
 * two IDataSets pointed to by two server-actors. It holds a reference
 * to the client-actor to which the results must be sent.
 * Triggered by a ZipOperation at a remote actor pointing a data-set.
 */
class ZipExecute implements Serializable{
    final ZipOperation zipOp;
    final IDataSet dataSet;
    final ActorRef clientActor;

    ZipExecute(final ZipOperation zipOperation,
               final IDataSet dataSet,
               final ActorRef sourceActor) {
        this.zipOp = zipOperation;
        this.dataSet = dataSet;
        this.clientActor = sourceActor;
    }
}
