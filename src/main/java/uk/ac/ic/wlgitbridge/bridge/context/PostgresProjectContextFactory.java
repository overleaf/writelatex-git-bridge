package uk.ac.ic.wlgitbridge.bridge.context;

import uk.ac.ic.wlgitbridge.bridge.db.postgres.PostgresConnectionPool;
import uk.ac.ic.wlgitbridge.bridge.db.postgres.PostgresProjectContext;
import uk.ac.ic.wlgitbridge.bridge.lock.OperationTracker;
import uk.ac.ic.wlgitbridge.bridge.lock.PostgresLock;
import uk.ac.ic.wlgitbridge.util.Log;

public class PostgresProjectContextFactory implements ProjectContextFactory {

    private PostgresConnectionPool connectionPool;
    private OperationTracker operationTracker;

    public PostgresProjectContextFactory(PostgresConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        this.operationTracker = new OperationTracker();
    }

    @Override
    public ProjectContext makeProjectContext(String projectName) {
        Log.info("[{}] Making project context (Postgres)", projectName);
        PostgresLock lock = new PostgresLock(projectName, this.operationTracker);
        PostgresProjectContext context = new PostgresProjectContext(
                lock,
                this.connectionPool,
                projectName
        );
        return context;
    }

    @Override
    public void close() {
        Log.info("Closing PostgresProjectContextFactory");
        this.connectionPool.close();
        this.operationTracker.close();
    }
}
