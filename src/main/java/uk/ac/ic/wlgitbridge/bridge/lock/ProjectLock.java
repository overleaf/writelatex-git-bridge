package uk.ac.ic.wlgitbridge.bridge.lock;

/**
 * Project Lock class.
 *
 * The locks should be re-entrant. For example, we are usually holding the lock
 * when a project must be restored, which tries to acquire the lock again.
 */
public interface ProjectLock {
    void lock();
    void unlock();

    default LockGuard lockGuard() {
        this.lock();
        return () -> this.unlock();
    }
}
