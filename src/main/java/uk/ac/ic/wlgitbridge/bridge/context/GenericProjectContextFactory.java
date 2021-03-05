package uk.ac.ic.wlgitbridge.bridge.context;

import uk.ac.ic.wlgitbridge.bridge.lock.InProcessLock;
import uk.ac.ic.wlgitbridge.bridge.lock.OperationTracker;
import uk.ac.ic.wlgitbridge.util.Log;

public class GenericProjectContextFactory implements ProjectContextFactory {

    private OperationTracker operationTracker;

    public GenericProjectContextFactory() {
        this.operationTracker = new OperationTracker();
    }

    @Override
    public ProjectContext makeProjectContext(String projectName) {
        Log.info("[{}] Making project context (Generic)", projectName);
        InProcessLock lock = new InProcessLock(projectName, this.operationTracker);
        GenericProjectContext context = new GenericProjectContext(lock, projectName);
        return context;
    }

    @Override
    public void close() {
        Log.info("Closing GenericProjectContextFactory");
        this.operationTracker.close();
    }
}
