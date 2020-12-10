package uk.ac.ic.wlgitbridge.bridge.db.postgres;

public class PostgresOptions {

  private String url;
  private String username;
  private String password;

  // Connection Pool options
  private int poolInitialSize;
  private int poolMaxTotal;
  private int poolMaxWaitMillis;

  public PostgresOptions(
    String url,
    String username,
    String password,
    int initialSize,
    int maxTotal,
    int maxWaitMillis) {
    this.url = url;
    this.username = username;
    this.password = password;
    if (initialSize < 1) {
      this.poolInitialSize = 1;
    } else {
      this.poolInitialSize = initialSize;
    }
    if (maxTotal < 1) {
      this.poolMaxTotal = 1;
    } else {
      this.poolMaxTotal = maxTotal;
    }
    if (maxWaitMillis < 1) {
      this.poolMaxWaitMillis = 1000;
    } else {
      this.poolMaxWaitMillis = maxWaitMillis;
    }
  }

  public String getUrl() {
    return this.url;
  }

  public String getUsername() {
    return this.username;
  }

  public String getPassword() {
    return this.password;
  }

  public int getPoolInitialSize() {
      return this.poolInitialSize;
  }

  public int getPoolMaxTotal() {
    return this.poolMaxTotal;
  }

  public int getPoolMaxWaitMillis() {
    return this.poolMaxWaitMillis;
  }
}
