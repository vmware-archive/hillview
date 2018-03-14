/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.dataset.remoting;

import com.google.protobuf.ByteString;
import org.hillview.pb.Command;
import org.hillview.pb.PartialResponse;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to hold memoized results from remote commands.
 */
public class MemoizedResults {
    /**
     * This is used as a element in the memoizedCommands hashmap below.
     */
    static class ResponseAndId {
        /**
         * The serialized version of the result of a computation.
         */
        final PartialResponse response;
        /**
         * If the partial response contains an IDataset index,
         * this field contains the raw index.
         */
        final Integer localDatasetIndex;

        ResponseAndId(final PartialResponse response, int index) {
            this.response = response;
            this.localDatasetIndex = index;
        }
    }

    /**
     * Map each (command, dataset index) to a partial response obtained by
     * running the command on that respective dataset.
     */
    private final ConcurrentHashMap<ByteString, Map<Integer, ResponseAndId>> memoizedCommands;

    MemoizedResults() {
        this.memoizedCommands = new ConcurrentHashMap<ByteString, Map<Integer, ResponseAndId>>();
    }

    /**
     * Purges all memoized results
     */
    public void clear() {
        this.memoizedCommands.clear();
    }

    @Nullable
    public ResponseAndId get(final Command command) {
        ByteString ser = command.getSerializedOp();
        if (!this.memoizedCommands.containsKey(ser))
            return null;
        Map<Integer, ResponseAndId> map = this.memoizedCommands.get(ser);
        int index = command.getIdsIndex();
        if (!map.containsKey(index))
            return null;
        return map.get(index);
    }

    public void insert(final Command command, final PartialResponse response, Integer index) {
        ResponseAndId rid = new ResponseAndId(response, index);
        this.memoizedCommands.computeIfAbsent(command.getSerializedOp(),
                (k) -> new ConcurrentHashMap<Integer, ResponseAndId>()).put(command.getIdsIndex(), rid);
    }

    public void remove(final Command command, final ResponseAndId resp) {
        Map<Integer, ResponseAndId> map = this.memoizedCommands.get(command.getSerializedOp());
        map.remove(command.getIdsIndex());
    }
}
