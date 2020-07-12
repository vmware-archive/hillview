package org.hillview.targets;

import javax.annotation.Nullable;

import org.hillview.HillviewComputation;
import org.hillview.HillviewRpc;
import org.hillview.RpcRequest;
import org.hillview.RpcRequestContext;
import org.hillview.dataStructures.DistinctCountRequestInfo;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.sketches.results.TableSummary;
import org.hillview.storage.CassandraConnectionInfo;
import org.hillview.storage.CassandraDatabase;
import org.hillview.storage.CassandraSSTableLoader;
import org.hillview.storage.ColumnLimits;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.CountWithConfidence;

/**
 *
 */
public class CassandraDBTarget extends TableRpcTarget {
    private static final long serialVersionUID = 1L;
    final CassandraConnectionInfo conn;
    protected final CassandraDatabase database;
    protected final CassandraSSTableLoader ssTableLoader;
    protected final int rowCount;
    protected final String ssTablePath;
    @Nullable
    protected Schema schema;
    private final ColumnLimits columnLimits;


    protected CassandraDBTarget(CassandraConnectionInfo conn, HillviewComputation computation) {
        super(computation);
        this.conn = conn;
        this.schema = null;
        this.registerObject();
        this.database = new CassandraDatabase(this.conn);
        this.columnLimits = new ColumnLimits();
        System.out.println("CassandraDBTarget init");

        try {
            this.rowCount = this.database.getRowCount();
            this.ssTablePath = this.database.getSSTablePath();
            this.ssTableLoader = new CassandraSSTableLoader(ssTablePath, this.conn.lazyLoading);
            this.schema = this.ssTableLoader.getSchema();
            // The table table is actually not used for anything; the only purpose
            // is for some APIs to be similar to the TableTarget class.
            SmallTable empty = new SmallTable(this.schema);
            this.setTable(new LocalDataSet<ITable>(empty));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "Local database: " + this.conn.toString();
    }

    @HillviewRpc
    public void getSummary(RpcRequest request, RpcRequestContext context) {
        TableSummary summary = new TableSummary(this.schema, this.rowCount);
        this.runSketch(this.table, new PrecomputedSketch<ITable, TableSummary>(summary), request, context);
    }

}