package uk.ac.ic.wlgitbridge.bridge.db.postgres;

import uk.ac.ic.wlgitbridge.bridge.context.ProjectContext;
import uk.ac.ic.wlgitbridge.bridge.lock.ProjectLock;
import java.sql.*;
import uk.ac.ic.wlgitbridge.bridge.lock.PostgresLock;
import uk.ac.ic.wlgitbridge.util.Log;

/**
 * A Postgres context uses an open connection to take and
 * maintain a lock on a row in the "projects" table.
 */
public class PostgresProjectContext implements ProjectContext {
  private String projectName;
  private PostgresLock lock;
  private Connection connection;

    /**
     * Take a connection from a connection pool, use it to
     * initialize the lock, which takes a database lock on
     * the appropriate row in the "projects" table.
     *
     * This connection can then be used to perform queries
     * for this project, until the lock is released.
     */
  public PostgresProjectContext(PostgresLock lock, PostgresConnectionPool pool, String projectName) {
      this.projectName = projectName;
      try {
          this.connection = pool.getConnection();
      } catch (SQLException e) {
          Log.error("[{}] Could not get connection from pool: {}", projectName, e.getMessage());
          throw new RuntimeException("Could not get connection from pool", e);
      }
      this.lock = lock;
      this.lock.setConnection(this.connection);
  }

  @Override
  public String getProjectName() {
    return this.projectName;
  }

  @Override
  public ProjectLock getLock() {
    return this.lock;
  }

    /**
     * Unique to PostgresProjectContext, get the underlying
     * connection, to perform database queries under the
     * lock on the project row
     */
  public Connection getConnection() {
    return this.connection;
  }

  @Override
  public void close() {
    try {
        this.lock.unlock();
    } catch (Exception e) {
        Log.error("[{}] Error while closing context (Postgres) - {}", this.projectName, e.getMessage());
    }
  }
}
