package uk.ac.ic.wlgitbridge.bridge.lock;

public interface ProjectLockFactory {
  public ProjectLock GetNewLock(String projectId);
}
