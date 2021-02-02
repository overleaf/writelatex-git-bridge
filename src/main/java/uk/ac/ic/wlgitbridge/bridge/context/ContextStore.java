package uk.ac.ic.wlgitbridge.bridge.context;

import uk.ac.ic.wlgitbridge.bridge.lock.ProjectLockFactory;

public class ContextStore {
  private ProjectLockFactory projectLockFactory;
  private DatabaseConnectionFactory databaseConnectionFactory;
  static private ContextStore instance;
  private boolean shuttingDown;

  private ContextStore(ProjectLockFactory projectLockFactory, DatabaseConnectionFactory databaseConnectionFactory) {
    this.projectLockFactory = projectLockFactory;
    this.databaseConnectionFactory = databaseConnectionFactory;
    this.shuttingDown = false;
  }

  static public void Initialize(ProjectLockFactory projectLockFactory, DatabaseConnectionFactory databaseConnectionFactory) {
    if (ContextStore.instance != null) {
      throw new ContextStoreAlreadyInitialisedException();
    }

    ContextStore.instance = new ContextStore(projectLockFactory, databaseConnectionFactory);
  }

  static public ContextStore Instance() {
    if (ContextStore.instance == null) {
      throw new ContextStoreNotInitialisedException();
    }
    return ContextStore.instance;
  }

  public Context GetContextForProject(String projectId) {
    if (there is one in local storage) {
      context = this.GetFromThreadLocalStorage();
      if (context.GetProjectId() != projectId) {
        throw WrongProjectIdException();
      }
      return context;
    }
    if (shuttingDown) {
      // TODO: catch this and return 502
      throw new BridgeShuttingDownException();
    }
    context = new Context(projectLockFactory.createLock(), databaseConnectionFactory.getConnection());
    this.StoreInThreadLocalStorage(context);
    return context;
  }
}
