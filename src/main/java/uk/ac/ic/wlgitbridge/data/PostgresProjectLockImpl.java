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
  // threads that currently hold a lock. This allows us to wait on
  // the wlock, effectively waiting for all outstanding operations to
  // complete, before shutting down
  private final ReentrantReadWriteLock rwlock;
  private final Lock rlock;
  private final ReentrantReadWriteLock.WriteLock wlock;

  // Postgres connection pool
  private final BasicDataSource pool;

  private LockAllWaiter waiter;
  private boolean waiting;

  public PostgresProjectLockImpl(BasicDataSource connectionPool) {
    projectLocks = new HashMap<String, Lock>();
    rwlock = new ReentrantReadWriteLock();
    rlock = rwlock.readLock();
    wlock = rwlock.writeLock();
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

  @Override
  public void lockForProject(String projectName) {
    rlock.lock();
    Lock lock = getLockForProjectName(projectName);
    lock.lock();
    projectLocks.put(projectName, lock);
  }

  @Override
  public void unlockForProject(String projectName) {
    Lock lock = projectLocks.get(projectName);
    if (lock == null) {
      return;
    }
    projectLocks.remove(projectName);
    lock.unlock();
    rlock.unlock();
    if (waiting) {
      trySignal();
    }
  }

  private void trySignal() {
    int threads = rwlock.getReadLockCount();
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
    wlock.lock();
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
