package org.hillview.sketches;

import org.hillview.dataset.IncrementalTableSketch;
import org.hillview.sketches.results.SampleSet;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

public class NumericSamplesSketch extends
        IncrementalTableSketch<SampleSet, ColumnWorkspace<ReservoirSampleWorkspace>> {
    private final String column;
    private final int sampleCount;
    private final long seed;

    public NumericSamplesSketch(String column, int sampleCount, long seed) {
        this.column = column;
        this.sampleCount = sampleCount;
        this.seed = seed;
    }

    @Override
    public void add(ColumnWorkspace<ReservoirSampleWorkspace> workspace, SampleSet result, int rowNumber) {
        if (workspace.column.isMissing(rowNumber))
            result.addMissing();
        else {
            double value = workspace.column.asDouble(rowNumber);
            result.add(workspace.childWorkspace, value);
        }
    }

    @Override
    public ColumnWorkspace<ReservoirSampleWorkspace> initialize(ITable data) {
        IColumn col = Converters.checkNull(data.getLoadedColumn(this.column));
        ReservoirSampleWorkspace workspace = new ReservoirSampleWorkspace(this.sampleCount, this.seed);
        return new ColumnWorkspace<ReservoirSampleWorkspace>(col, workspace);
    }

    @Nullable
    @Override
    public SampleSet zero() {
        return new SampleSet(this.sampleCount, this.seed);
    }

    @Nullable
    @Override
    public SampleSet add(@Nullable SampleSet left, @Nullable SampleSet right) {
        return Converters.checkNull(left).add(Converters.checkNull(right));
    }
}
