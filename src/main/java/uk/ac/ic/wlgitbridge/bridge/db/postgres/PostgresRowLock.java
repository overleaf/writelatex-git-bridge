package uk.ac.ic.wlgitbridge.bridge.db.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class PostgresRowLock implements Lock {

  private String projectName;
  private PostgresDBStore dbStore;
  private boolean isLocking = false;

  public PostgresRowLock(String projectName, PostgresDBStore dbStore) {
    this.projectName = projectName;
    this.dbStore = dbStore;
  }

  @Override
  public void lock() {
    try {
      dbStore.takeLock(projectName);
      this.isLocking = true;
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
      dbStore.releaseLock(projectName);
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error while releasing lock", e);
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
