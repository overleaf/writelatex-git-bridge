package uk.ac.ic.wlgitbridge.bridge.context;

public interface ProjectContextFactory {
    ProjectContext makeProjectContext(String projectName);
    void close();
}
