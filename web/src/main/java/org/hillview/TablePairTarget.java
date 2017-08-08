package org.hillview;

import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.SetOperationMap;
import org.hillview.table.api.ITable;

import javax.websocket.Session;

/**
 * This is a remote object that has a reference to an IDataSet[Pair[ITable,ITable]]
 */
class TablePairTarget extends RpcTarget {
    private final IDataSet<Pair<ITable, ITable>> tables;
    TablePairTarget(IDataSet<Pair<ITable, ITable>> tables) {
        this.tables = tables;
    }

    @HillviewRpc
    void setOperation(RpcRequest request, Session session) {
        String op = request.parseArgs(String.class);
        SetOperationMap sm = new SetOperationMap(op);
        this.runMap(this.tables, sm, TableTarget::new, request, session);
    }
}
