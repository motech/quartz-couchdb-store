package org.motechproject.quartz;

/**
   Quartz job store uses couchdb name based on current OS user name of the quartz process.
 */
public class UserScopedDatabaseNameProvider implements DatabaseNameProvider {
    public String getDatabaseName() {
       return System.getProperty("user.name") + "-scheduler";
    }
}
