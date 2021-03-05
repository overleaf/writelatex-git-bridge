package uk.ac.ic.wlgitbridge.bridge.lock;

import uk.ac.ic.wlgitbridge.bridge.util.Pair;
import uk.ac.ic.wlgitbridge.util.Log;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thin wrapper around a ReentrantLock
 */
public class InProcessLock implements ProjectLock {

  /**
   * Static map of underlying locks, for reference counting.
   * Each project has one ReentrantLock (a "base" lock), and
   * an associated reference count.
   */
  private static HashMap<String, Pair<ReentrantLock, Integer>> countedLocks =
          new HashMap<>();

  private String projectName;
  private ReentrantLock lock;
  private OperationTracker registry;

  public InProcessLock(String projectName, OperationTracker operationTracker) {
    this.projectName = projectName;
    this.registry = operationTracker;
    this.lock = getBaseLock(projectName);
  }

  /**
   * Get a base lock and increment its reference count,
   * or create a new one, inserting into countedLocks
   * @param projectName
   */
  private synchronized ReentrantLock getBaseLock(String projectName) {
    Pair<ReentrantLock, Integer> lockPair;
    if (InProcessLock.countedLocks.containsKey(projectName)) {
      lockPair = InProcessLock.countedLocks.get(projectName);
      Integer newCount = lockPair.updateRight((Integer i) -> ++i);
      Log.debug("[{}] Increment base lock: {}", projectName, newCount);
    } else {
      Log.debug("[{}] New base lock", projectName);
      lockPair = new Pair<>(new ReentrantLock(), 1);
      InProcessLock.countedLocks.put(projectName, lockPair);
    }
    return lockPair.getLeft();
  }

  /**
   * Decrement the reference count on a base lock.
   * If the count goes to zero, remove the lock
   * from countedLocks
   * @param projectName
   */
  private synchronized void decrementLock(String projectName) {
    Pair<ReentrantLock, Integer> lockPair;
    if (InProcessLock.countedLocks.containsKey(projectName)) {
      lockPair = InProcessLock.countedLocks.get(projectName);
      Integer newCount = lockPair.updateRight((Integer i) -> --i);
      Log.debug("[{}] Decrement base lock: {}", projectName, newCount);
      if (newCount <= 0) {
        Log.debug("[{}] Removing base lock", projectName);
        lockPair.updateLeft((_l) -> null);
        InProcessLock.countedLocks.remove(projectName);
      }
    }
  }

  /**
   * Finalizer, runs before an InProcessLock object is
   * garbage collected.
   */
  @Override
  public void finalize() {
    Log.debug("[{}] Finalize InProcessLock", this.projectName);
    decrementLock(this.projectName);
  }

  @Override
  public void lock() {
    Log.info("[{}] Trying to get lock", this.projectName);
    this.registry.lock();
    this.lock.lock();
    Log.info("[{}] Got lock", this.projectName);
  }

  @Override
  public void unlock() {
    Log.info("[{}] Unlocking", this.projectName);
    try {
      this.lock.unlock();
    } catch (Exception e) {
      // Already unlocked, ignore
    }
    this.registry.unlock();
  }

  @Override
  public void close() {
      this.unlock();
  }

  /**
   * Success doesn't matter in this case
   */
  @Override
  public void success() {
    return;
  }

}
