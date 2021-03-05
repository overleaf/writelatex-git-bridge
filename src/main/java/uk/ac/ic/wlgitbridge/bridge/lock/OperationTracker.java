package uk.ac.ic.wlgitbridge.bridge.lock;

import uk.ac.ic.wlgitbridge.data.LockAllWaiter;
import uk.ac.ic.wlgitbridge.util.Log;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Keeps track of "operations" in flight, for the purposes of
 * shutting down the server cleanly.
 */
public class OperationTracker {

    private final ReentrantReadWriteLock rwlock;
    private final Lock rlock;
    private final ReentrantReadWriteLock.WriteLock wlock;
    private LockAllWaiter waiter;
    private boolean waiting;

    public OperationTracker() {
        rwlock = new ReentrantReadWriteLock();
        rlock = rwlock.readLock();
        wlock = rwlock.writeLock();
        waiting = false;
        this.waiter = (int threads) ->
                Log.info("Waiting for " + threads + " projects...");
    }

    /**
     * Takes a read lock, effectively registering the current thread
     * as an "operation" to wait on if/when the server shuts down
     */
    public synchronized void lock() {
        if (waiting) {
            throw new RuntimeException("Waiting for OperationTracker to close");
        }
        rlock.lock();
    }

    /**
     * Releases the read lock for the current thread, effectively
     * ending this "operation"
     */
    public synchronized void unlock() {
        try {
            rlock.unlock();
        } catch (Exception e) { }
        if (waiting) {
            trySignal();
        }
    }

    /**
     * Called when the server is shutting down.
     * Taking the write lock will wait until all read
     * locks have been released, in other words, wait
     * until all current "operations" have finished.
     */
    public synchronized void close() {
        Log.info("Closing LockRegistry");
        waiting = true;
        trySignal();
        wlock.lock();
        wlock.unlock();
    }

    private synchronized void trySignal() {
        int threads = rwlock.getReadLockCount();
        if (waiter != null && threads > 0) {
            waiter.threadsRemaining(threads);
        }
    }

}
