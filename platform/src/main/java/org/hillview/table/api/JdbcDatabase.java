package org.hillview.table.api;

import org.hillview.table.*;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogManager;

import javax.annotation.Nullable;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Conversions between JDBC information and ITable objects.
 */
public class JdbcDatabase {
    private final JdbcConnectionInformation connInfo;
    @Nullable
    private Connection connection;

    public JdbcDatabase(final JdbcConnectionInformation connInfo) {
        this.connInfo = connInfo;
        this.connection = null;
    }

    public void connect() throws SQLException {
        String url = this.connInfo.getURL();
        HillviewLogManager.instance.logger.log(Level.INFO, "Database server url=" + url);
        this.connection = DriverManager.getConnection(
                url, this.connInfo.user, this.connInfo.password);
    }

    public void disconnect() throws SQLException {
        if (this.connection == null)
            return;
        this.connection.close();
        this.connection = null;
    }

    public ResultSet getTable(String table) {
        String query = "SELECT * FROM " + table;
        try {
            return this.getQueryResult(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet getQueryResult(String query) throws SQLException {
        Statement st = Converters.checkNull(this.connection).createStatement();
        return st.executeQuery(query);
    }

    static ColumnDescription getDescription(ResultSetMetaData meta, int colIndex)
            throws SQLException {
        colIndex = colIndex + 1;

        String name = meta.getColumnLabel(colIndex);
        boolean allowMissing = false;
        ContentsKind kind;

        switch (meta.isNullable(colIndex)) {
            case ResultSetMetaData.columnNullable:
            case ResultSetMetaData.columnNullableUnknown:
                allowMissing = true;
                break;
            case ResultSetMetaData.columnNoNulls:
                break;
            default:
                throw new RuntimeException("Unexpected isNullable value");
        }

        int colType = meta.getColumnType(colIndex);
        switch (colType) {
            case Types.BOOLEAN:
            case Types.BIT:
                kind = ContentsKind.Category;
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                kind = ContentsKind.Integer;
                break;
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                kind = ContentsKind.Double;
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                kind = ContentsKind.String;
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                kind = ContentsKind.Date;
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.NULL:
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.REF:
            case Types.DATALINK:
            case Types.ROWID:
            case Types.NCLOB:
            case Types.REF_CURSOR:
            default:
                throw new RuntimeException("Unhandled column type " + colType);
        }
        return new ColumnDescription(name, kind, allowMissing);
    }

    static Schema getSchema(ResultSet data) {
        try {
            ResultSetMetaData meta = data.getMetaData();
            Schema result = new Schema();
            for (int i = 0; i < meta.getColumnCount(); i++) {
                ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
                result.append(cd);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static ITable getTable(ResultSet data) {
        try {
            ResultSetMetaData meta = data.getMetaData();
            List<IAppendableColumn> cols = new ArrayList<IAppendableColumn>();
            for (int i = 0; i < meta.getColumnCount(); i++) {
                ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
                BaseListColumn col = BaseListColumn.create(cd);
                cols.add(col);
            }

            int rowsRead = 0;
            while (data.next()) {
                rowsRead++;
                if (rowsRead % 50000 == 0)
                    System.out.print(".");
                for (int i = 0; i < cols.size(); i++) {
                    int colIndex = i + 1;
                    IAppendableColumn col = cols.get(i);
                    int colType = meta.getColumnType(colIndex);
                    switch (colType) {
                        case Types.BOOLEAN:
                        case Types.BIT:
                            boolean b = data.getBoolean(colIndex);
                            if (data.wasNull())
                                col.appendMissing();
                            else
                                col.append(b ? "true" : "false");
                            break;
                        case Types.TINYINT:
                        case Types.SMALLINT:
                        case Types.INTEGER:
                            int integer = data.getInt(colIndex);
                            if (data.wasNull())
                                col.appendMissing();
                            else
                                col.append(integer);
                            break;
                        case Types.BIGINT:
                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            double d = data.getDouble(colIndex);
                            if (data.wasNull())
                                col.appendMissing();
                            else
                                col.append(d);
                            break;
                        case Types.CHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                        case Types.SQLXML:
                            String s = data.getString(colIndex);
                            col.append(s);
                            break;
                        case Types.DATE:
                        case Types.TIME:
                        case Types.TIMESTAMP:
                        case Types.TIME_WITH_TIMEZONE:
                        case Types.TIMESTAMP_WITH_TIMEZONE:
                            Timestamp ts = data.getTimestamp(colIndex);
                            LocalDateTime ld = ts.toLocalDateTime();
                            col.append(ld);
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                        case Types.NULL:
                        case Types.OTHER:
                        case Types.JAVA_OBJECT:
                        case Types.DISTINCT:
                        case Types.STRUCT:
                        case Types.ARRAY:
                        case Types.BLOB:
                        case Types.CLOB:
                        case Types.REF:
                        case Types.DATALINK:
                        case Types.ROWID:
                        case Types.NCLOB:
                        case Types.REF_CURSOR:
                        default:
                            throw new RuntimeException("Unhandled column type " + colType);
                    }
                }
            }
            return new Table(cols);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
