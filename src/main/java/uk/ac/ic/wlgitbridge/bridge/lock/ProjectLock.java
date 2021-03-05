package uk.ac.ic.wlgitbridge.bridge.lock;

/**
 * A Lock on a Project.
 *
 * This does not implement the Lock interface, because we need
 * to distinguish success from failure. These need to
 * be handled differently when using our PostgresProjectLock.
 *
 * We implement AutoCloseable so we can play nice with
 * the try-with-resources statement.
 */
public interface ProjectLock extends AutoCloseable {
    void lock();
    void unlock();
    void close();
    void success();
}
