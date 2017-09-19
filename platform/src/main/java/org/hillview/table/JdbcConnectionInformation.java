package org.hillview.table;

import org.apache.http.client.utils.URIBuilder;

import java.io.Serializable;

/**
 * This information is required to open a database connection.
 */
public class JdbcConnectionInformation implements Serializable {
    public final String host;
    public final String database;
    public final int port;
    public final String user;
    public final String password;
    public final String dbkind;

    public JdbcConnectionInformation(String host, String database, String user, String password) {
        this.host = host;
        this.user = user;
        this.database = database;
        this.password = password;
        this.port = 3306;
        this.dbkind = "mysql";
    }

    public String getURL() {
        URIBuilder builder = new URIBuilder();
        builder.setHost(this.host);
        builder.setPort(this.port);
        builder.setScheme("jdbc:" + this.dbkind);
        builder.setPath(this.database);
        builder.addParameter("useSSL", "false");
        builder.addParameter("serverTimeZone", "PDT");
        return builder.toString();
    }
}
