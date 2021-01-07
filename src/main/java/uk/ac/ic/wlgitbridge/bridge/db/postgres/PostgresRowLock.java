package uk.ac.ic.wlgitbridge.bridge.db.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class PostgresRowLock implements Lock {

  private String projectName;
  private Connection connection;
  private boolean isLocking = false;

  public PostgresRowLock(String projectName, Connection conn) {
    this.projectName = projectName;
    this.connection = conn;
  }

  @Override
  public void lock() {
    try {
      connection.setAutoCommit(true);
      try (
        PreparedStatement statement = this.connection.prepareStatement(
          "INSERT into project_locks (project_name) values (?) ON CONFLICT DO NOTHING;"
        )
      ) {
        statement.setString(1, this.projectName);
        statement.execute();
      }
      try (Statement statement = this.connection.createStatement()) {
        statement.execute("set lock_timeout to 59999;");
      }
      this.connection.setAutoCommit(false);
      this.isLocking = true;
      try (
        PreparedStatement statement = this.connection.prepareStatement(
          "SELECT * from project_locks" +
            " WHERE project_name = ?" +
            " FOR UPDATE;"
        );
        ) {
        statement.setString(1, this.projectName);
        statement.executeQuery();
      }
      return;
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error while locking " + this.projectName, e);
    }
  }

  @Override
  public void unlock() {
    if (!this.isLocking) {
      throw new RuntimeException("unlock called, but lock is not taken");
    }
    try {
      this.connection.commit();
      this.connection.setAutoCommit(true);
      this.connection.close();
      return;
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    throw new InterruptedException("not possible");
  }

  @Override
  public boolean tryLock() {
    return false;
  }

  @Override
  public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
    return false;
  }

  @Override
  public Condition newCondition() {
    return null;
  }
}
