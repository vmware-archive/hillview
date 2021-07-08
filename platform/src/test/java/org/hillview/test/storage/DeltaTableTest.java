package org.hillview.test.storage;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.maps.ExtractWorkerFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.sketches.SummarySketch;
import org.hillview.sketches.results.TableSummary;
import org.hillview.storage.IFileReference;
import org.hillview.storage.delta.DeltaTableDescription;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DeltaTableTest extends BaseTest {
    private static final Path deltaTablePath = Paths.get(dataDir, "ontime", "delta-table");

    @Test
    public void readTest() {
        Empty e = Empty.getInstance();
        LocalDataSet<Empty> local = new LocalDataSet<>(e);
        DeltaTableDescription description = new DeltaTableDescription();
        description.path = deltaTablePath.toString();
        description.snapshotVersion = null;
        Map<String, List<String>> filesPerWorker = Collections.singletonMap(Utilities.getHostName(), description.getFiles());
        IMap<Empty, List<IFileReference>> finder = new ExtractWorkerFilesMap<>(filesPerWorker);
        IDataSet<IFileReference> found = local.blockingFlatMap(finder);

        IMap<IFileReference, ITable> loader = new LoadFilesMap();
        IDataSet<ITable> mapped = found.blockingMap(loader);
        SummarySketch sk = new SummarySketch();
        TableSummary tableSummary = mapped.blockingSketch(sk);
        Assert.assertEquals(tableSummary.rowCount, 1_315_543);
    }
}
