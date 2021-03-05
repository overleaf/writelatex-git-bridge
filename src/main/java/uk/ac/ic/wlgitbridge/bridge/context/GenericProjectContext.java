package uk.ac.ic.wlgitbridge.bridge.context;

import uk.ac.ic.wlgitbridge.bridge.lock.ProjectLock;

public class GenericProjectContext implements ProjectContext {
  private String projectName;
  private ProjectLock lock;

  public GenericProjectContext(ProjectLock lock, String projectName) {
    this.projectName = projectName;
    this.lock = lock;
  }

  @Override
  public String getProjectName() {
    return this.projectName;
  }

  @Override
  public ProjectLock getLock() {
    return this.lock;
  }

  @Override
  public void close() {
      return;
  }
}
