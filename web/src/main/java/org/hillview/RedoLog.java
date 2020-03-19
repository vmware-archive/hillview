/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview;

import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;
import org.hillview.dataset.api.IJson;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The Redo log stores information about how RpcTarget objects have been created.
 * The log can be stored to persistent storage.
 * On start-up the log is read from persistent storage allowing sessions
 * to persist across system restarts.
 */
class RedoLog {
    private static final String defaultStorageFile = "hillview.redo";

    /**
     * For each object id the computation that has produced it.
     */
    private final HashMap<RpcTarget.Id, HillviewComputation> generator =
            new HashMap<RpcTarget.Id, HillviewComputation>();
    /**
     * Map object id to object.
     */
    private final HashMap<RpcTarget.Id, RpcTarget> objects;
    /**
     * File storing this redo log.  If null there is no persistent storage.
     */
    @Nullable
    private final String backupFile;

    RedoLog() {
        this.objects = new HashMap<RpcTarget.Id, RpcTarget>();
        this.backupFile = defaultStorageFile;
        this.reload();
    }

    synchronized public void addObject(RpcTarget target) {
        if (this.objects.containsKey(target.getId()))
            throw new RuntimeException("Object with id " + target.getId() + " already in map");
        this.generator.put(target.getId(), target.computation);
        this.persistInLog(target);
        HillviewLogger.instance.info("Inserting targetId", "{0}", target.toString());
        this.objects.put(target.getId(), target);
    }

    private static class DestAndRequest implements IJson {
        static final long serialVersionUID = 1;

        final String resultId;
        final RpcRequest request;

        DestAndRequest(String resultId, RpcRequest request) {
            this.request = request;
            this.resultId = resultId;
        }
    }

    private synchronized void addGenerated(String id, RpcRequest request) {
        HillviewLogger.instance.info("Installing object lineage", "{0} from {1}",
                id, request);
        RpcTarget.Id tid = new RpcTarget.Id(id);
        this.generator.put(tid, new HillviewComputation(tid, request));
    }

    private void reload() {
        if (this.backupFile == null)
            return;
        HillviewLogger.instance.info("Replaying redo log");
        try (FileReader fr = new FileReader(this.backupFile)) {
             JsonStreamParser parser = new JsonStreamParser(fr);
             while (parser.hasNext()) {
                 JsonElement elem = parser.next();
                 DestAndRequest dar = IJson.gsonInstance.fromJson(elem, DestAndRequest.class);
                 this.addGenerated(dar.resultId, dar.request);
             }
        } catch (IOException ex) {
            HillviewLogger.instance.error("Cannot read hillview redo log", ex);
        }
    }

    private void persistInLog(RpcTarget target) {
        // Yes, we write the data and close the file immediately.
        if (this.backupFile == null)
            return;
        if (target.computation == null)
            return;
        DestAndRequest dar = new DestAndRequest(
                target.getId().toString(),
                target.computation.request);
        try (FileWriter f = new FileWriter(this.backupFile, true);
             PrintWriter p = new PrintWriter(f)) {
            p.println(dar.toJson());
        } catch (IOException ex) {
            HillviewLogger.instance.error("Cannot write to redo log", ex);
        }
    }

    synchronized @Nullable
    RpcTarget getObject(RpcTarget.Id id) {
        HillviewLogger.instance.info("Getting object", "{0}", id);
        return this.objects.get(id);
    }

    synchronized void deleteObject(RpcTarget.Id id) {
        if (id.isInitial()) {
            HillviewLogger.instance.error("Cannot delete object 0");
            return;
        }
        if (!this.objects.containsKey(id))
            throw new RuntimeException("Object with id " + id + " does not exist");
        this.objects.remove(id);
    }

    /**
     * Removes all RemoteObjects from the cache, except the specified (initial) object.
     * @return  The number of objects removed.
     */
    public int removeAllObjects(RpcTarget.Id except) {
        List<RpcTarget.Id> toDelete = new ArrayList<RpcTarget.Id>();
        for (RpcTarget.Id k: this.objects.keySet()) {
            if (!k.equals(except))
                toDelete.add(k);
        }

        for (RpcTarget.Id k: toDelete)
            this.deleteObject(k);
        return toDelete.size();
    }

    @Nullable
    public HillviewComputation getComputation(RpcTarget.Id id) {
        return this.generator.get(id);
    }
}
