package uk.ac.ic.wlgitbridge.bridge.lock;

import uk.ac.ic.wlgitbridge.bridge.lock.InProcessLock;

import java.util.HashMap;

public final class InProcessLockFactory implements ProjectLockFactory {
  private final HashMap<String, InProcessLock> locks;

  public InProcessLockFactory() {
    this.locks = new HashMap<String, InProcessLock>();
  }

  public synchronized ProjectLock GetNewLock(String projectId) {
    InProcessLock lock = this.locks.get(projectId);
    if (lock != null) {
      return lock;
    }

    lock = new InProcessLock();
    // TODO: We're going to need to remove this from 'locks' eventually
    // Does this need to be reference counted?
    return lock;
  }
}
