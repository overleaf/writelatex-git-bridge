package uk.ac.ic.wlgitbridge.bridge.db.sqlite;

import uk.ac.ic.wlgitbridge.bridge.db.DatabaseConfig;

public class SqliteConfig implements DatabaseConfig {

  public SqliteConfig() {};

  @Override
  public DatabaseType getDatabaseType() {
    return DatabaseType.SQLite;
  }

  @Override
  public DatabaseConfig asSanitized() {
    return new SqliteConfig();
  }
}
