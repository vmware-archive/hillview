package org.hillview.test.storage;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.maps.FindDeltaTableFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.sketches.SummarySketch;
import org.hillview.sketches.results.TableSummary;
import org.hillview.storage.IFileReference;
import org.hillview.storage.delta.DeltaTableDescription;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class DeltaTableTest extends BaseTest {
    private static final Path deltaTablePath = Paths.get(dataDir, "ontime", "delta-table");

    @Test
    public void readTest() {
        if (!Files.isDirectory(deltaTablePath)) {
            this.ignoringException("Cannot find test delta table", new FileNotFoundException(deltaTablePath.toString()));
            return;
        }

        Empty e = Empty.getInstance();
        LocalDataSet<Empty> local = new LocalDataSet<>(e);
        DeltaTableDescription description = new DeltaTableDescription();
        description.path = deltaTablePath.toString();
        description.snapshotVersion = null;
        IMap<Empty, List<IFileReference>> finder = new FindDeltaTableFilesMap<>(description);
        IDataSet<IFileReference> found = local.blockingFlatMap(finder);

        IMap<IFileReference, ITable> loader = new LoadFilesMap();
        IDataSet<ITable> mapped = found.blockingMap(loader);
        SummarySketch sk = new SummarySketch();
        TableSummary tableSummary = mapped.blockingSketch(sk);
        Assert.assertEquals(1_315_543, tableSummary.rowCount);
    }
}
