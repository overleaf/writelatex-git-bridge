package uk.ac.ic.wlgitbridge.bridge.lock;

import java.util.concurrent.locks.ReentrantLock;

public class InProcessLock implements ProjectLock {
  private ReentrantLock lock;

  public InProcessLock() {
    this.lock = new ReentrantLock();
  }

  public void lock() {
    this.lock.lock();
  }

  public void unlock() {
    this.lock.unlock();
  }
}
