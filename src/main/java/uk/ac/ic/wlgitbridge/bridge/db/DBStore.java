package uk.ac.ic.wlgitbridge.bridge.db;

import uk.ac.ic.wlgitbridge.bridge.db.postgres.PostgresConfig;
import uk.ac.ic.wlgitbridge.bridge.db.postgres.PostgresDBStore;
import uk.ac.ic.wlgitbridge.bridge.db.postgres.PostgresOptions;
import uk.ac.ic.wlgitbridge.bridge.db.sqlite.SqliteDBStore;
import uk.ac.ic.wlgitbridge.bridge.repo.RepoStore;
import uk.ac.ic.wlgitbridge.util.Log;

import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

/**
 * Created by winston on 20/08/2016.
 */
public interface DBStore {

    enum RequestEnd { Commit, Rollback };

    void prepareRequest() throws SQLException;

    void endRequest(RequestEnd end) throws SQLException;

    int getNumProjects();

    List<String> getProjectNames();

    void setLatestVersionForProject(String project, int versionID);

    int getLatestVersionForProject(String project);

    void addURLIndexForProject(String projectName, String url, String path);

    void deleteFilesForProject(String project, String... files);

    String getPathForURLInProject(String projectName, String url);

    String getOldestUnswappedProject();

    void swap(String projectName, String compressionMethod);

    void restore(String projectName);

    String getSwapCompression(String projectName);

    int getNumUnswappedProjects();

    void setRequestEnd(String end);

    void close();

    ProjectState getProjectState(String projectName);

    /**
     * Sets the last accessed time for the given project name.
     * @param projectName the project's name
     * @param time the time, or null if the project is to be swapped
     */
    void setLastAccessedTime(String projectName, Timestamp time);

    static DBStore fromConfig(Optional<DatabaseConfig> maybeConfig, RepoStore repoStore) {
      if (
        maybeConfig.isPresent() &&
        maybeConfig.get().getDatabaseType() == DatabaseConfig.DatabaseType.Postgres
      ) {
        Log.info("Database: connect to postgres");
        PostgresOptions options = ((PostgresConfig)maybeConfig.get()).getOptions();
        return new PostgresDBStore(options);
      } else {
        Log.info("Database: connect to sqlite");
        return new SqliteDBStore(
          Paths.get(
            repoStore.getRootDirectory().getAbsolutePath()
          ).resolve(".wlgb").resolve("wlgb.db").toFile()
        );
      }
    }
}
