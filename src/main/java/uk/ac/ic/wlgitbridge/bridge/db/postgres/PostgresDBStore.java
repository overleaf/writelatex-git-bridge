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
  public void close() {
    try {
      pool.close();
    } catch (Exception e) {
      throw new RuntimeException("Postgres error while closing", e);
    }
    return;
  }

  @Override
  public int getNumProjects() {
    try (
      Connection connection = pool.getConnection();
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
  public List<String> getProjectNames() {
    try (
      Connection connection = pool.getConnection();
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
  public void setLatestVersionForProject(String project, int versionID) {
    try (
      Connection connection = pool.getConnection();
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
  public int getLatestVersionForProject(String project) {
    try (
      Connection connection = pool.getConnection();
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
  public void addURLIndexForProject(String projectName, String url, String path) {
    try (
      Connection connection = pool.getConnection();
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
  public void deleteFilesForProject(String project, String... files) {
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
      Connection connection = pool.getConnection();
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
  public String getPathForURLInProject(String projectName, String url) {
    try (
      Connection connection = pool.getConnection();
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
  public String getOldestUnswappedProject() {
    try (
      Connection connection = pool.getConnection();
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
  public int getNumUnswappedProjects() {
    try (
      Connection connection = pool.getConnection();
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
  public ProjectState getProjectState(String projectName) {
    try (
      Connection connection = pool.getConnection();
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
  public void setLastAccessedTime(String projectName, Timestamp time) {
    try (
      Connection connection = pool.getConnection();
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
}
