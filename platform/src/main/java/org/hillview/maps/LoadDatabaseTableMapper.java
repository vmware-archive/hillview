package org.hillview.maps;

import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IMap;
import org.hillview.table.JdbcConnectionInformation;
import org.hillview.table.api.ITable;
import org.hillview.table.api.JdbcDatabase;

import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LoadDatabaseTableMapper implements IMap<Empty, ITable> {
    /**
     * Name of SQL table to load.
     */
    private final String tableName;
    private final JdbcConnectionInformation conn;

    public LoadDatabaseTableMapper(String tableName, JdbcConnectionInformation conn) {
        this.tableName = tableName;
        this.conn = conn;
    }

    @Override
    public ITable apply(Empty data) {
        try {
            JdbcDatabase db = new JdbcDatabase(this.conn);
            db.connect();
            ResultSet rs = db.getTable(this.tableName);
            ITable result = JdbcDatabase.getTable(rs);
            db.disconnect();
            return result;
        } catch (SQLException|MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
