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

package org.hillview.test.dataset;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.PartialResult;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;
import rx.Observable;

import java.util.List;

public class LoadRaceTest extends BaseTest {
    // Test for issue #318
    @Test
    public void load() throws InterruptedException {
        Empty e = new Empty();
        LocalDataSet<Empty> local = new LocalDataSet<Empty>(e);
        FileSetDescription desc = new FileSetDescription();
        desc.fileKind = "csv";
        desc.fileNamePattern = "../data/ontime/*_1.csv";
        desc.headerRow = true;
        IMap<Empty, List<IFileReference>> finder = new FindFilesMap(desc);
        IDataSet<IFileReference> found = local.blockingFlatMap(finder);

        IMap<IFileReference, ITable> loader = new LoadFilesMap();
        Thread t0 = runInBackground(found, loader);
        Thread.sleep(1000);
        Thread t1 = runInBackground(found, loader);

        t0.join();
        t1.join();
    }

    private Thread runInBackground(IDataSet<IFileReference> found, IMap<IFileReference, ITable> loader) {
        Runnable first = () -> {
            Observable<PartialResult<IDataSet<ITable>>> files = found.map(loader);
            IDataSet<ITable> table = files.toBlocking().last().deltaValue;
            Assert.assertNotNull(table);
        };
        Thread t = new Thread(first);
        t.start();
        return t;
    }
}
