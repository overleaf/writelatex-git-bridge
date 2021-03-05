package uk.ac.ic.wlgitbridge.bridge.db.postgres;

import uk.ac.ic.wlgitbridge.bridge.context.*;
import uk.ac.ic.wlgitbridge.bridge.db.DBStore;
import uk.ac.ic.wlgitbridge.bridge.db.ProjectState;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresDBStore implements DBStore {

  private PostgresConnectionPool pool;

  public PostgresDBStore(PostgresConnectionPool connectionPool) {
      this.pool = connectionPool;
  }

  /**
   * Get the connection from project context. The connection _should_
   * already have an open transaction, with a lock on the project row.
   * @see uk.ac.ic.wlgitbridge.bridge.lock.PostgresLock
   * @see ContextStore:inContextWithLock(...)
   *
   * This should always be used when we are operation on a specific project
   *
   * The connection should _not_ be closed after it has been used
   * for a query. It will be committed and closed later when the
   * lock is released.
   */
  private Connection connectionFromContext(String project) {
    PostgresProjectContext context = (PostgresProjectContext) ContextStore.getInstance().getContextForProject(project);
    Connection connection = context.getConnection();
    return connection;
  }

  /**
   * Get a connection directly from the connection pool.
   *
   * This should be used when we don't have a specific project
   * to operate on.
   */
  private Connection connectionFromPool() throws SQLException {
    return pool.getConnection();
  }

  @Override
  public int getNumProjects() {
    try (
      Connection connection = this.connectionFromPool();
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
      Connection connection = this.connectionFromPool();
      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery("SELECT name from projects;\n")
    ) {
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
    try {
      Connection connection = connectionFromContext(project);
      try (PreparedStatement statement = connection.prepareStatement(
              "INSERT INTO "
                      + "projects (name, version_id, last_accessed) "
                      + "VALUES (?, ?, now()) "
                      + "ON CONFLICT (name) DO UPDATE "
                      + "SET version_id = EXCLUDED.version_id, last_accessed = now();\n")) {

        statement.setString(1, project);
        statement.setInt(2, versionID);
        statement.executeUpdate();
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public int getLatestVersionForProject(String project) {
      try {
        Connection connection = connectionFromContext(project);
        try (
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
        }
      } catch (Exception e) {
        throw new RuntimeException("Postgres query error", e);
      }
  };

  @Override
  public void addURLIndexForProject(String projectName, String url, String path) {
    try {
      Connection connection = connectionFromContext(projectName);
      try (PreparedStatement statement = connection.prepareStatement(
              "INSERT INTO url_index_store(" +
                      "project_name, " +
                      "url, " +
                      "path" +
                      ") VALUES " +
                      "(?, ?, ?)" +
                      "ON CONFLICT (project_name,path) DO UPDATE " +
                      "SET url = EXCLUDED.url;\n")
              ) {
        statement.setString(1, projectName);
        statement.setString(2, url);
        statement.setString(3, path);
        statement.executeUpdate();
      }
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
    try {
      Connection connection = connectionFromContext(project);
      try (PreparedStatement statement = connection.prepareStatement(queryBuilder.toString())) {
        statement.setString(1, project);
        for (int i = 0; i < files.length; i++) {
          statement.setString(i + 2, files[i]);
        }
        statement.execute();
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public String getPathForURLInProject(String projectName, String url) {
    try {
      Connection connection = connectionFromContext(projectName);
      try (PreparedStatement statement = connection.prepareStatement(
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
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };

  @Override
  public String getOldestUnswappedProject() {
    try (
      Connection connection = connectionFromPool();
      Statement statement = connection.createStatement()
    ) {
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
      Connection connection = connectionFromPool();
      Statement statement = connection.createStatement()
    ) {
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
    try {
      Connection connection = connectionFromContext(projectName);
      try (PreparedStatement statement = connection.prepareStatement(
              "SELECT version_id, last_accessed\n" +
                      " FROM projects\n" +
                      " WHERE name = ?;\n")) {

        statement.setString(1, projectName);
        try (ResultSet rs = statement.executeQuery()) {
          ProjectState result;
          if (rs.next()) {
            if (rs.getInt(1) == 0) {
              // Account for postgres row lock
              result = ProjectState.NOT_PRESENT;
            } else if (rs.getTimestamp(2) == null) {
              result = ProjectState.SWAPPED;
            } else {
              result = ProjectState.PRESENT;
            }
          } else {
            result = ProjectState.NOT_PRESENT;
          }
          return result;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  };


  @Override
  public void setLastAccessedTime(String projectName, Timestamp time) {
    try {
      Connection connection = connectionFromContext(projectName);
      try (PreparedStatement statement = connection.prepareStatement(
              "UPDATE projects\n" +
                      "SET last_accessed = ?\n" +
                      "WHERE name = ?;\n"
      )) {
        statement.setTimestamp(1, time);
        statement.setString(2, projectName);
        statement.executeUpdate();
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  }

  @Override
  public void swap(String projectName, String compressionMethod) {
    try {
      Connection connection = connectionFromContext(projectName);
      try (PreparedStatement statement = connection.prepareStatement(
              "UPDATE projects\n" +
                      "SET last_accessed = NULL,\n" +
                      "    swap_time = NOW(),\n" +
                      "    restore_time = NULL,\n" +
                      "    swap_compression = ?\n" +
                      " WHERE name = ?;\n"
      )) {
        statement.setString(1, compressionMethod);
        statement.setString(2, projectName);
        statement.executeUpdate();
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  }

  @Override
  public void restore(String projectName) {
    try {
      Connection connection = connectionFromContext(projectName);
      try (PreparedStatement statement = connection.prepareStatement(
              "UPDATE projects\n" +
                      "SET last_accessed = NOW(),\n" +
                      "    swap_time = NULL,\n" +
                      "    restore_time = NOW(),\n" +
                      "    swap_compression = NULL\n" +
                      " WHERE name = ?;\n"
      )) {
        statement.setString(1, projectName);
        statement.executeUpdate();
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  }

  @Override
  public String getSwapCompression(String projectName) {
    try {
      Connection connection = connectionFromContext(projectName);
      try (PreparedStatement statement = connection.prepareStatement(
              "SELECT swap_compression \n"
                      + "FROM projects \n"
                      + "WHERE name = ?;\n")) {
        statement.setString(1, projectName);
        try (ResultSet rs = statement.executeQuery()) {
          if (rs.next()) {
            String compression = rs.getString(1);
            return compression;
          } else {
            return null;
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Postgres query error", e);
    }
  }
}
