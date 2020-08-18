package org.hillview.test.storage;

import org.hillview.storage.HiveConnectionInfo;
import org.hillview.storage.HiveDatabase;
import org.hillview.storage.HiveHDFSLoader;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.junit.Test;

public class HiveTest extends BaseTest {

    public HiveConnectionInfo getConnectionInfo() {
        HiveConnectionInfo conn = new HiveConnectionInfo();
        conn.host = "localhost";
        conn.port = 10000;
        conn.database = "default";
        conn.dataDelimiter = "\u0001";
        // conn.dataDelimiter = "0";
        // conn.dataDelimiter = "/";
        // conn.dataDelimiter = "/t";
        // conn.table = "tb3";
        conn.table = "invites";
        // conn.table = "no_par";
        // conn.table = "test_delim";
        conn.databaseKind = "Hive";
        conn.user = "";
        conn.password = "";
        conn.hdfsNodes = "localhost";
        conn.namenodePort = "9000";
        conn.hadoopUsername = "daniar";
        conn.lazyLoading = true;
        return conn;
    }

    @Test
    public void testHiveConnection() {
        HiveConnectionInfo conn;
        HiveDatabase db;
        try {
            conn = this.getConnectionInfo();
            db = new HiveDatabase(conn);
            db.closeConnection();
        } catch (Exception e) {
            // this will fail if local Hive and HDFS is not running, but we don't want to
            // fail the test.
            this.ignoringException("Failed connecting to Hive Database", e);
            return;
        }
    }

    @Test
    public void testHiveHDFSLoader() {
        HiveConnectionInfo conn;
        HiveDatabase db;
        try {
            conn = this.getConnectionInfo();
            db = new HiveDatabase(conn);
            db.closeConnection();
            // System.out.println(db.toString());
        } catch (Exception e) {
            // this will fail if local Hive and HDFS is not running, but we don't want to
            // fail the test.
            this.ignoringException("Failed connecting to Hive Database", e);
            return;
        }
        HiveHDFSLoader hiveHDFSLoader = new HiveHDFSLoader(conn, db.hadoopUGI, db.tableSchema,
                db.metadataColumn, db.arrPartitions, db.hdfsInetAddresses);
        ITable table = hiveHDFSLoader.load();
        System.out.println("table " + table);
    }

}