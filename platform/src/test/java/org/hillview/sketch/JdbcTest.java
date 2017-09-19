package org.hillview.sketch;

import org.hillview.table.JdbcConnectionInformation;
import org.hillview.table.api.ITable;
import org.hillview.table.api.JdbcDatabase;
import org.junit.Test;

import java.sql.ResultSet;

public class JdbcTest {
    @Test
    public void testJdbc() {
        JdbcConnectionInformation conn =
                new JdbcConnectionInformation("localhost", "employees", "mbudiu", "password");
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
            ResultSet salaries = db.getTable("salaries");
            ITable table = JdbcDatabase.getTable(salaries);
            db.disconnect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            System.out.println("Cannot connect to database");
            e.printStackTrace();
        }
    }
}
