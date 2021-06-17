package uk.ac.ic.wlgitbridge.bridge.db;

public interface DatabaseConfig {
  enum DatabaseType {SQLite, Postgres};

  DatabaseType getDatabaseType();

  public DatabaseConfig asSanitized();

}
