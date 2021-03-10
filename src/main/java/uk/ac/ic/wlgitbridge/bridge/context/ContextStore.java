package uk.ac.ic.wlgitbridge.bridge.context;

import uk.ac.ic.wlgitbridge.bridge.lock.ProjectLock;
import uk.ac.ic.wlgitbridge.bridge.util.Pair;
import uk.ac.ic.wlgitbridge.bridge.util.ThrowingFunction;
import uk.ac.ic.wlgitbridge.util.Log;
import java.util.HashMap;
import java.util.function.Function;

/**
 * A singleton that manages Project Contexts in the application.
 * Contexts are wrappers around a ProjectLock, and any other
 * project-specific resources that need to be managed during
 * an "operation" on a project.
 *
 * Contexts are kept in a ThreadLocal variable, so they can
 * be re-used once they have been created.
 *
 * The intended use is to open a context at the 'top' of a
 * git operation (push, pull, swap), and lock the project.
 * Then, deeper in the call-stack the same context can be
 * retrieved and used.
 *
 * (In practice, this enables us to use a Postgres connection
 * to lock a row in the database, and hold a transaction open,
 * to be used further down the call stack.)
 */
public class ContextStore {
  private ProjectContextFactory projectContextFactory;
  static private ContextStore instance;
  private boolean shuttingDown;

  private ThreadLocal<HashMap<String, ProjectContext>> localContexts;

  private ContextStore(ProjectContextFactory contextFactory) {
    this.projectContextFactory = contextFactory;
    this.shuttingDown = false;
    this.localContexts = new ThreadLocal<HashMap<String, ProjectContext>>();
  }

  /**
   * Must be called early in application life-cycle.
   * - @see GenericProjectContextFactory
   * - @see PostgresProjectContextFactory
   */
  public static void initialize(ProjectContextFactory contextFactory) throws ContextStoreAlreadyInitialisedException {
    Log.info("Initialize ContextStore");
    if (ContextStore.instance != null) {
        Log.error("ContextStore already initialized");
      throw new ContextStoreAlreadyInitialisedException();
    }
    ContextStore.instance = new ContextStore(contextFactory);
  }

  /**
   * Reset the ContextStore, only to be used in tests,
   * hence the __scary__ underscores
   */
  public static void __Reset__() {
    Log.info("Reset ContextStore");
    if (ContextStore.instance != null) {
      ContextStore.getInstance().stop();
      ContextStore.instance = null;
    }
  }

  public static ContextStore getInstance()  {
    if (ContextStore.instance == null) {
      throw new RuntimeException("ContextStore not initialized");
    }
    return ContextStore.instance;
  }

  /**
   * This method takes a lambda, and runs it in a
   * context with the project lock taken, and automatically
   * cleans up at the end, returning a Pair<R, Exception>
   *
   * This pattern is used to get around a limitation of
   * how lambdas interact with the exception system.
   *
   * @see uk.ac.ic.wlgitbridge.bridge.Bridge for usage.
   *
   * @param projectName : String project name
   * @param f : A lambda, accepting a context as input,
   *            and returning a Pair<R, Exception>
   * @param <R> : The return type of the overall expression
   * @return Pair<R, Exception>
   */
  public static <R> R inContextWithLock(
          String projectName,
          ThrowingFunction<ProjectContext, R, Exception> f
  ) throws Exception {
    ContextStore instance = ContextStore.getInstance();
    instance.ensureThreadLocalStorage();
    if (instance.localContexts.get().get(projectName) != null) {
      throw new RuntimeException("Context already exists");
    }
    ProjectContext context = instance.getContextForProject(projectName);
    try (ProjectLock lock = context.getLock()) {
      lock.lock();
      R result = f.apply(context);
      lock.success();
      return result;
    } catch (Exception e) {
      Log.error("[{}] Exception in context with lock: {}", projectName, e.getMessage());
      throw e;
    } finally {
      context.close();
      instance.localContexts.get().remove(projectName);
    }
  }

  /**
   * Cleanup contexts, and pass on the close signal.
   * Essential for cleanly shutting down the server
   */
  public synchronized void stop() {
    Log.info("Stopping ContextStore");
    this.clearLocalContexts();
    if (this.projectContextFactory != null) {
      this.projectContextFactory.close();
    }
  }

  private void ensureThreadLocalStorage() {
    if (this.localContexts.get() == null) {
      HashMap<String, ProjectContext> contextMap = new HashMap<String, ProjectContext>();
      this.localContexts.set(contextMap);
    }
  }

  private synchronized ProjectContext getFromThreadLocalStorage(String projectName) {
    this.ensureThreadLocalStorage();
    HashMap<String, ProjectContext> contextMap = this.localContexts.get();
    return contextMap.get(projectName);
  }

  private synchronized void storeInThreadLocalStorage(String projectName, ProjectContext context) {
    this.ensureThreadLocalStorage();
    HashMap<String, ProjectContext> contextMap = this.localContexts.get();
    contextMap.put(projectName, context);
  }

  public ProjectContext getContextForProject(String projectName) {
    ProjectContext storedContext = this.getFromThreadLocalStorage(projectName);
    if (storedContext != null) {
      if (!storedContext.getProjectName().equals(projectName)) {
        Log.error("Context ID does not match " + projectName + ", " + storedContext.getProjectName());
        throw new RuntimeException("Wrong project ID");
      }
      return storedContext;
    }
    if (shuttingDown) {
      throw new RuntimeException("ContextStore is shutting down");
    }
    ProjectContext newContext = this.projectContextFactory.makeProjectContext(projectName);
    this.storeInThreadLocalStorage(projectName, newContext);
    return newContext;
  }

  /**
   * Clean up all contexts used on this thread.
   */
  public void clearLocalContexts() {
    ensureThreadLocalStorage();
    HashMap<String, ProjectContext> contexts = this.localContexts.get();
    contexts.forEach((s, projectContext) -> {
              projectContext.close();
            }
    );
    contexts.clear();
    this.localContexts.remove();
  }
}
