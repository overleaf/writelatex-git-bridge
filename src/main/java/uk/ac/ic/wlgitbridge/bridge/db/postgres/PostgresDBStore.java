package uk.ac.ic.wlgitbridge.bridge.db.postgres;

import org.apache.commons.dbcp2.BasicDataSource;
import org.postgresql.jdbc2.optional.ConnectionPool;
import uk.ac.ic.wlgitbridge.bridge.db.DBInitException;
import uk.ac.ic.wlgitbridge.bridge.db.DBStore;
import uk.ac.ic.wlgitbridge.bridge.db.ProjectState;
import uk.ac.ic.wlgitbridge.util.Log;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresDBStore implements DBStore {

  private final PostgresOptions options;
  private final BasicDataSource pool;
  private ThreadLocal<Connection> connectionBox = new ThreadLocal();
  private ThreadLocal<RequestEnd> requestEndBox = new ThreadLocal();

  public PostgresDBStore(PostgresOptions postgresOptions) {
    options = postgresOptions;
    Log.info("Initialize PostgresDBStore");
    try {
      pool = makeConnectionPool();
    } catch (Exception e) {
      Log.error("Error connecting to Postgres: {}", e.getMessage());
      throw new DBInitException(e);
    }
  }

  // TODO: make private
  public BasicDataSource makeConnectionPool() {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName("org.postgresql.Driver");
    dataSource.setUrl(options.getUrl());
    dataSource.setUsername(options.getUsername());
    dataSource.setPassword(options.getPassword());
    int poolInitialSize = options.getPoolInitialSize();
    int poolMaxTotal = options.getPoolMaxTotal();
    int poolMaxWaitMillis = options.getPoolMaxWaitMillis();
    if (poolInitialSize < 1) {
      throw new RuntimeException("Invalid poolInitialSize: " + poolInitialSize);
    }
    if (poolMaxTotal < poolInitialSize) {
      throw new RuntimeException("Invalid poolMaxTotal and poolInitialSize: " + poolMaxTotal + ", " + poolInitialSize);
    }
    dataSource.setInitialSize(poolInitialSize);
    dataSource.setMaxTotal(poolMaxTotal);
    dataSource.setMaxWaitMillis(poolMaxWaitMillis);
    return dataSource;
  }

  @Override
  public synchronized void prepareRequest() throws SQLException {
    Log.info("Preparing database for request");
    // TODO:
    //   - turn off autocommit
    //   - start transaction
    Connection conn = pool.getConnection();
    conn.setAutoCommit(true);
    try (Statement statement = conn.createStatement()) {
      statement.execute("set lock_timeout to 59999;");
    }
    conn.setAutoCommit(false);
    connectionBox.set(conn);
    requestEndBox.set(null);
  }

  // TODO: should we commit or rollback?
  @Override
  public synchronized void endRequest(RequestEnd end) throws SQLException {
    Log.info("Ending request on database");
    Connection conn = connectionBox.get();
    if (end == RequestEnd.Commit) {
      conn.commit();
    } else if (end == RequestEnd.Rollback) {
      conn.rollback();
    }
    conn.close();
  }

  private Connection getConnection() {
    Connection connection = connectionBox.get();
    if (connection == null) {
      throw new RuntimeException("Tried to get connection, but not initialized");
    }
    return connectionBox.get();
  }

  @Override
  public void close() {
    try {
      pool.close();
    } catch (Exception e) {
      throw new RuntimeException("Postgres error while closing", e);
    }
    return;
  }

  @Override
  public synchronized int getNumProjects() {
    try (
      Connection connection = getConnection();
      Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery("SELECT count(*) from projects;\n")) {
      if (rs.next()) {
        int result = rs.getInt(1);
        return result;
      } else {
        return 0;
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public synchronized List<String> getProjectNames() {
    try (
      Connection connection = getConnection();
      Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery("SELECT name from projects;\n")) {
      List<String> result = new ArrayList<String>();
      while (rs.next()) {
        result.add(rs.getString(1));
      }
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public synchronized void setLatestVersionForProject(String project, int versionID) {
    try (
      Connection connection = getConnection();
      PreparedStatement statement = connection.prepareStatement(
        "INSERT INTO "
           + "projects (name, version_id, last_accessed) "
           + "VALUES (?, ?, now()) "
           + "ON CONFLICT (name) DO UPDATE "
           + "SET version_id = EXCLUDED.version_id, last_accessed = now();\n")) {
      statement.setString(1, project);
      statement.setInt(2, versionID);
      statement.executeUpdate();
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public synchronized int getLatestVersionForProject(String project) {
    try (
      Connection connection = getConnection();
      PreparedStatement statement = connection.prepareStatement(
        "SELECT version_id from projects where name = ?;")) {
      statement.setString(1, project);
      try (ResultSet rs = statement.executeQuery()) {
        if (rs.next()) {
          int versionId = rs.getInt(1);
          return versionId;
        } else {
          return 0;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public synchronized void addURLIndexForProject(String projectName, String url, String path) {
    try (
      Connection connection = getConnection();
      PreparedStatement statement = connection.prepareStatement(
      "INSERT INTO url_index_store(" +
         "project_name, " +
         "url, " +
         "path" +
         ") VALUES " +
         "(?, ?, ?)" +
         "ON CONFLICT (project_name,path) DO UPDATE " +
         "SET url = EXCLUDED.url;\n")) {
      statement.setString(1, projectName);
      statement.setString(2, url);
      statement.setString(3, path);
      statement.executeUpdate();
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public synchronized void deleteFilesForProject(String project, String... files) {
    if (files.length == 0) {
      return;
    }
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("DELETE FROM url_index_store ");
    queryBuilder.append("WHERE project_name = ? AND path IN (");
    for (int i = 0; i < files.length; i++) {
      queryBuilder.append("?");
      if (i < files.length - 1) {
        queryBuilder.append(", ");
      }
    }
    queryBuilder.append(");\n");
    try (
      Connection connection = getConnection();
      PreparedStatement statement = connection.prepareStatement(queryBuilder.toString())) {
      statement.setString(1, project);
      for (int i = 0; i < files.length; i++) {
        statement.setString(i + 2, files[i]);
      }
      statement.execute();
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public synchronized String getPathForURLInProject(String projectName, String url) {
    try (
      Connection connection = getConnection();
      PreparedStatement statement = connection.prepareStatement(
        "SELECT path "
          + "FROM url_index_store "
          + "WHERE project_name = ? "
          + "AND url = ?;\n")) {
      statement.setString(1, projectName);
      statement.setString(2, url);
      try (ResultSet rs = statement.executeQuery()) {
        if (rs.next()) {
          String path = rs.getString(1);
          return path;
        } else {
          return null;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public synchronized String getOldestUnswappedProject() {
    try (
      Connection connection = getConnection();
      Statement statement = connection.createStatement()) {
      try (ResultSet rs = statement.executeQuery(
          "SELECT name FROM projects" +
            " ORDER BY last_accessed ASC" +
            " LIMIT 1;\n")) {
        if (rs.next()) {
          String project = rs.getString(1);
          return project;
        } else {
          return null;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public synchronized int getNumUnswappedProjects() {
    try (
      Connection connection = getConnection();
      Statement statement = connection.createStatement()) {
      try (ResultSet rs = statement.executeQuery(
          "SELECT COUNT(*)\n" +
            " FROM projects\n" +
            " WHERE last_accessed IS NOT NULL;\n")) {
        if (rs.next()) {
          int n = rs.getInt(1);
          return n;
        } else {
          return 0;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public synchronized ProjectState getProjectState(String projectName) {
    try (
      Connection connection = getConnection();
      PreparedStatement statement = connection.prepareStatement(
        "SELECT last_accessed\n" +
          " FROM projects\n" +
          " WHERE name = ?;\n")) {
      statement.setString(1, projectName);
      try (ResultSet rs = statement.executeQuery()) {
        ProjectState result;
        if (rs.next()) {
          if (rs.getTimestamp(1) == null) {
            result = ProjectState.SWAPPED;
          } else {
            result = ProjectState.PRESENT;
          }
        } else {
          result = ProjectState.NOT_PRESENT;
        }
        return result;
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public synchronized void setLastAccessedTime(String projectName, Timestamp time) {
    try (
      Connection connection = getConnection();
      PreparedStatement statement = connection.prepareStatement(
        "UPDATE projects\n" +
          "SET last_accessed = ?\n" +
          "WHERE name = ?;\n"
      )) {
      statement.setTimestamp(1, time);
      statement.setString(2, projectName);
      statement.executeUpdate();
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  public synchronized void takeLock(String projectName) {
    Connection connection = getConnection();
    try {
      connection.setAutoCommit(true);
      try (
              PreparedStatement statement = connection.prepareStatement(
                      "INSERT into projects (name, version_id) values (?, 0) ON CONFLICT DO NOTHING;"
              )
      ) {
        statement.setString(1, projectName);
        statement.execute();
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute("set lock_timeout to 59999;");
      }
      connection.setAutoCommit(false);
      try (
              PreparedStatement statement = connection.prepareStatement(
                      "SELECT * from project" +
                              " WHERE name = ?" +
                              " FOR UPDATE;"
              );
      ) {
        statement.setString(1, projectName);
        statement.executeQuery();
      }
      return;
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error while locking " + projectName, e);
    }
  }

  public synchronized void releaseLock(String projectName) {
    RequestEnd requestEnd = requestEndBox.get();
    Connection connection = getConnection();
    try {
      if (requestEnd == RequestEnd.Commit) {
        connection.commit();
      } else if (requestEnd == RequestEnd.Rollback) {
        connection.rollback();
      } else {
        throw new RuntimeException("[{}] Invalid request end: " + requestEnd.toString());
      }
      connection.setAutoCommit(true);
      connection.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // TODO: not a string
  @Override
  public void setRequestEnd(String end) {
    if ("commit".equals(end)) {
      requestEndBox.set(RequestEnd.Commit);
    } else if ("rollback".equals(end)) {
      requestEndBox.set(RequestEnd.Rollback);
    } else {
      throw new RuntimeException("invalid request end: " + end);
    }
  }
}
