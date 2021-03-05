package uk.ac.ic.wlgitbridge.bridge.context;

import uk.ac.ic.wlgitbridge.bridge.lock.ProjectLock;

public interface ProjectContext {
  String getProjectName();
  ProjectLock getLock();
  void close();
}
