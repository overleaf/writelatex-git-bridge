package uk.ac.ic.wlgitbridge.bridge.context;

import uk.ac.ic.wlgitbridge.bridge.lock.InProcessLock;
import uk.ac.ic.wlgitbridge.bridge.lock.ProjectLock;
import uk.ac.ic.wlgitbridge.bridge.lock.ProjectLockFactory;

public class GenericProjectContext implements ProjectContext {
  private String projectId;
  private ProjectLock lock;

  public GenericProjectContext(ProjectLockFactory lockFactory, String projectId) {
    this.projectId = projectId;
    this.lock = lockFactory.GetNewLock(projectId);
  }

  @Override
  public String GetProjectId() {
    return this.projectId;
  }

  @Override
  public ProjectLock GetLock() {
    return this.lock;
  }
}
