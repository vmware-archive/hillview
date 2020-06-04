package org.hillview.sketches;

import org.hillview.sketches.highorder.PostProcessedSketch;
import org.hillview.sketches.results.NextKList;
import org.hillview.table.AggregateDescription;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewException;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Extends the functionality of the NextKSketch by computing some aggregates using post-processing.
 */
public class NextKSketchAggregate extends PostProcessedSketch<ITable, NextKList, NextKList> {
    @Nullable
    private final AggregateDescription[] aggregates;

    public NextKSketchAggregate(NextKSketch sketch, @Nullable AggregateDescription[] aggregates) {
        super(sketch);
        this.aggregates = aggregates;
    }

    private static IColumn divideColumns(String name, IColumn sum, IColumn count) {
        ColumnDescription cd = new ColumnDescription(name, ContentsKind.Double);
        int rows = sum.sizeInRows();
        DoubleArrayColumn avg = new DoubleArrayColumn(cd, rows);
        for (int j = 0; j < rows; j++) {
            if (sum.isMissing(j))
                avg.setMissing(j);
            else {
                double s = sum.getDouble(j);
                double c = count.getDouble(j);
                avg.set(j, s / c);
            }
        }
        return avg;
    }

    @Nullable
    @Override
    public NextKList postProcess(@Nullable NextKList list) {
        if (list != null && list.aggregates != null) {
            // build here a new table
            List<IColumn> columns = new ArrayList<IColumn>();
            Converters.checkNull(this.aggregates);

            // This SmallTable contains the columns produced by getAggregates,
            // but we need the columns indicated by this.aggregates
            Schema schema = list.aggregates.getSchema();
            List<String> cols = schema.getColumnNames();

            AggregateDescription[] computed = AggregateDescription.getAggregates(this.aggregates);
            Converters.checkNull(computed);
            if (computed.length != cols.size())
                throw new HillviewException("The number of aggregate columns does not match: " +
                        computed.length + " vs " + cols.size());
            for (AggregateDescription a : this.aggregates) {
                // iterating over the requested aggregates
                if (a.agkind != AggregateDescription.AggregateKind.Average) {
                    int index = Utilities.indexOf(a, computed);
                    if (index < 0)
                        throw new HillviewException("Could not find aggregated column " + a);
                    IColumn col = list.aggregates.getColumn(cols.get(index));
                    columns.add(col);
                } else {
                    // Let's find the sum and count columns
                    IColumn sum = null;
                    IColumn count = null;
                    for (int j = 0; j < computed.length; j++) {
                        AggregateDescription o = computed[j];
                        if (!o.cd.equals(a.cd))
                            continue;
                        if (o.agkind == AggregateDescription.AggregateKind.Sum)
                            sum = list.aggregates.getColumn(cols.get(j));
                        else if (o.agkind == AggregateDescription.AggregateKind.Count)
                            count = list.aggregates.getColumn(cols.get(j));
                    }
                    if (sum == null)
                        throw new HillviewException(
                                "Could not find sum aggregate column for average");
                    if (count == null)
                        throw new HillviewException(
                                "Could not find count aggregate column for average");
                    String colname = schema.generateColumnName("Average(" + a.cd.name + ")");
                    IColumn avg = NextKSketchAggregate.divideColumns(colname, sum, count);
                    columns.add(avg);
                }
            }
            SmallTable aggregates = new SmallTable(columns);
            return new NextKList(
                    list.rows, aggregates, list.count, list.startPosition, list.rowsScanned);
        }
        return list;
    }
}
