package uk.ac.ic.wlgitbridge.data;

import org.apache.commons.dbcp2.BasicDataSource;
import uk.ac.ic.wlgitbridge.bridge.db.postgres.PostgresRowLock;
import uk.ac.ic.wlgitbridge.bridge.lock.ProjectLock;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PostgresProjectLockImpl implements ProjectLock {

  // Keep locks in a map, for lookup by name
  private final Map<String, Lock> projectLocks;

  // An app-level lock that tracks all current "operations", or,
  // threads that currently hold a lock on the activityLock. This allows
  // us to wait on the blockingLock, effectively waiting for all
  // outstanding operations to complete, before shutting down
  private final ReentrantReadWriteLock globalLock;
  private final Lock activityLock;
  private final ReentrantReadWriteLock.WriteLock blockingLock;

  // Postgres connection pool
  private final BasicDataSource pool;

  private LockAllWaiter waiter;
  private boolean waiting;

  public PostgresProjectLockImpl(BasicDataSource connectionPool) {
    projectLocks = new HashMap<String, Lock>();
    globalLock = new ReentrantReadWriteLock();
    activityLock = globalLock.readLock();
    blockingLock = globalLock.writeLock();
    pool = connectionPool;
    waiting = false;
  }

  public PostgresProjectLockImpl(BasicDataSource connectionPool, LockAllWaiter waiter) {
    this(connectionPool);
    setWaiter(waiter);
  }

  private PostgresRowLock getProjectLock(String projectName) {
    try {
      Connection connection = pool.getConnection();
      PostgresRowLock lock = new PostgresRowLock(projectName, connection);
      return lock;
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  }

  private synchronized Lock getLockForProjectName(String projectName) {
    return getProjectLock(projectName);
  }

  private synchronized void storeLock(String projectName, Lock lock) {
    projectLocks.put(projectName, lock);
  }

  private synchronized Lock retrieveLock(String projectName) {
    return projectLocks.get(projectName);
  }

  private synchronized void removeLock(String projectName) {
    projectLocks.remove(projectName);
  }

  @Override
  public void lockForProject(String projectName) {
    activityLock.lock();
    Lock lock = getLockForProjectName(projectName);
    lock.lock();
    storeLock(projectName, lock);
  }

  @Override
  public void unlockForProject(String projectName) {
    Lock lock = retrieveLock(projectName);
    if (lock == null) {
      return;
    }
    removeLock(projectName);
    lock.unlock();
    activityLock.unlock();
    if (waiting) {
      trySignal();
    }
  }

  private void trySignal() {
    int threads = globalLock.getReadLockCount();
    if (waiter != null && threads > 0) {
      waiter.threadsRemaining(threads);
    }
  }

  // lock the wlock, preventing any more threads from
  // getting the rlock (to start work). Used when stopping
  // the server
  public void lockAll() {
    waiting = true;
    trySignal();
    blockingLock.lock();
    try {
      pool.close();
    } catch (Exception e) {
      throw new RuntimeException(e); // TODO: better log
    }
  }

  public void setWaiter(LockAllWaiter waiter) {
    this.waiter = waiter;
  }
}
