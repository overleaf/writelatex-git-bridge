package uk.ac.ic.wlgitbridge.bridge.lock;

import uk.ac.ic.wlgitbridge.util.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * This ProjectLock class uses a database row lock in Postgres
 * as its implementation.
 */
public class PostgresLock implements ProjectLock {

    private String projectName;
    private Connection connection;
    private boolean isLocking;
    private boolean shouldCommit = false;
    private OperationTracker registry;

    public PostgresLock(String projectName, OperationTracker operationTracker) {
      this.projectName = projectName;
      this.registry = operationTracker;
      this.isLocking = false;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void lock() {
        Log.info("[{}] Trying to get lock", this.projectName);
        this.registry.lock();
        try {
            /**
             * Turn off auto-commit on this connection, so the next
             * query will begin a transaction
             */
            connection.setAutoCommit(false);
            /**
             * "touch" the project row, to ensure there is something to
             * take a lock on. Does nothing if the row already exists.
             *
             * Setting version_id = 0 is important, as it signals that the
             * project is not "really" present in the system.
             * @see PostgresDBStore.getProjectState()
             */
            try (
                    PreparedStatement statement = this.connection.prepareStatement(
                            "INSERT into projects (name, version_id, last_accessed) " +
                                    "values (?, 0, NOW()) ON CONFLICT DO NOTHING;"
                    )
            ) {
                statement.setString(1, this.projectName);
                statement.execute();
            }
            try (Statement statement = this.connection.createStatement()) {
                statement.execute("set lock_timeout to 59999;");
            }
            /**
             * Take a lock on the project row.
             * This transaction will remain open
             */
            this.isLocking = true;
            try (
                    PreparedStatement statement = this.connection.prepareStatement(
                            "SELECT * from projects" +
                                    " WHERE name = ?" +
                                    " FOR UPDATE;"
                    );
            ) {
                statement.setString(1, this.projectName);
                statement.executeQuery();
            }
            Log.info("[{}] Got lock", this.projectName);
            return;
        } catch (Exception e) {
            throw new RuntimeException("Postgres query error while locking " + this.projectName, e);
        }
    }

    @Override
    public void unlock() {
        if (!this.isLocking) {
            return;
        }
        try {
            /**
             * Unlock by committing the transaction,
             * or by rolling back, depending on whether
             * this boolean was set via .success()
             */
            if (this.shouldCommit == true) {
                Log.info("[{}] Unlocking with commit", this.projectName);
                this.connection.commit();
            } else {
                Log.info("[{}] Unlocking with rollback", this.projectName);
                this.connection.rollback();
            }
            this.connection.setAutoCommit(true);
            this.connection.close();
            this.isLocking = false;
            return;
        } catch (Exception e) {
            throw new RuntimeException("Postgres query error", e);
        } finally {
            this.registry.unlock();
        }
    }

    @Override
    public void close() {
        this.unlock();
    }

    @Override
    public void success() {
        this.shouldCommit = true;
    }

}
