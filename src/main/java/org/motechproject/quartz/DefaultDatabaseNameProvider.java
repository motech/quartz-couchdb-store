package org.motechproject.quartz;

public class DefaultDatabaseNameProvider implements DatabaseNameProvider {

    private String dbName;

    public DefaultDatabaseNameProvider(String dbName) {
        this.dbName = dbName;
    }

    public String getDatabaseName() {
        return dbName;
    }
}
