package org.motechproject.quartz;

public class DefaultDatabaseNameProvider implements DatabaseNameProvider {

    public String getDatabaseName() {
        return "quartz-couchdb-store";
    }
}
