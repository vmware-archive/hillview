package org.hillview.test.storage;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;
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
        // conn.table = "pokes";
        conn.table = "invites";
        // conn.table = "no_par";
        // conn.table = "test_delim";
        conn.databaseKind = "Hive";
        conn.user = "";
        conn.password = "";
        conn.hdfsNodes = "localhost";
        conn.namenodePort = "9000";
        conn.lazyLoading = true;
        return conn;
    }

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public void readTest(String url) throws Exception {
        InputStream in = null;
        try {
            in = new URL(url).openStream();
            // open and read from file
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = br.readLine();
            while (line != null) {
                System.out.println(line);
                line = br.readLine();
            }
            br.close();
            
            // IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    @Test
    public void readLocalHDFS() throws Exception {
        readTest("hdfs://localhost:9000/user/hive/warehouse/invites/ds=2008-08-08/kv5.txt");
    }

    // @Test
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
            System.out.println(db);
        } catch (Exception e) {
            // this will fail if local Hive and HDFS is not running, but we don't want to
            // fail the test.
            this.ignoringException("Failed connecting to Hive Database", e);
            return;
        }
        HiveHDFSLoader hiveHDFSLoader = new HiveHDFSLoader(conn, db.getTableSchema(),
                db.getMetadataColumn(), db.getArrPartitions(), db.getHdfsInetAddresses());
        ITable table = hiveHDFSLoader.load();
        System.out.println("table " + table);
    }

}