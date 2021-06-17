package uk.ac.ic.wlgitbridge.bridge.db.postgres;

import uk.ac.ic.wlgitbridge.bridge.db.DatabaseConfig;

public class PostgresConfig implements DatabaseConfig {
  private final PostgresOptions options;

  public PostgresConfig(PostgresOptions options) {
    this.options = options;
  }

  @Override
  public DatabaseType getDatabaseType() {
    return DatabaseType.Postgres;
  }

  public PostgresOptions getOptions() {
    return options;
  }

  @Override
  public DatabaseConfig asSanitized() {
    return new PostgresConfig(new PostgresOptions(
      "<REDACTED>",
      "<REDACTED>",
      "<REDACTED>",
      options.getPoolInitialSize(),
      options.getPoolMaxTotal(),
      options.getPoolMaxWaitMillis()
    ));
  }
}
