package org.motechproject.quartz;

/***
 * Implement this interface to change the database name used by quartz, useful when running multiple
 * instances of quartz in same server. see {@link UserScopedDatabaseNameProvider}
 */
public interface DatabaseNameProvider {
    String getDatabaseName();
}
