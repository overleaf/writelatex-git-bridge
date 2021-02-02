package uk.ac.ic.wlgitbridge.bridge.db.postgres;

import uk.ac.ic.wlgitbridge.bridge.context.ProjectContext;
import uk.ac.ic.wlgitbridge.bridge.lock.ProjectLock;
import java.sql.*;

public class PostgresProjectContext implements ProjectContext {
  private String projectId;
  private PostgresDBStore dbStore;
  private PostgresProjectLock lock;
  private Connection connection;

  public PostgresProjectContext(PostgresDBStore dbStore, String projectId) {
    this.dbStore = dbStore;
  }

  public String GetProjectId() {
    return this.projectId;
  }

  public ProjectLock GetLock() {
    if (this.lock != null) {
      return this.lock;
    }

    this.lock = new PostgresProjectLock(this.GetConnection());
    return this.lock;
  }

  public Connection GetConnection() {
    if (this.connection != null) {
      return this.connection;
    }

    this.connection = this.dbStore.GetConnectionFromPool();
    return connection;
  }
}
