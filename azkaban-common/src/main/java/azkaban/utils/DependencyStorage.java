package azkaban.utils;

import azkaban.db.DatabaseOperator;
import azkaban.project.ProjectManagerException;
import azkaban.spi.StartupDependencyDetails;
import azkaban.spi.Storage;
import java.io.File;
import java.io.IOException;
import javax.inject.Inject;


public class DependencyStorage {
  private final DatabaseOperator dbOperator;
  private final Storage storage;

  @Inject
  DependencyStorage(final DatabaseOperator dbOperator, final Storage storage) {
    this.storage = storage;
    this.dbOperator = dbOperator;
  }

  public boolean dependencyExists(final StartupDependencyDetails d, final String cacheKey) {
    // See if our in-memory cache of dependencies in storage already has this dependency listed
    // If it does, no need to download! It must already be in storage.
    if (dependenciesInStorage.contains(d)) return false;

    // Check if the dependency exists in storage
    try {
      if (this.storage.existsDependency(d.getFile(), d.getSHA1())) {
        // It does, so we need to update our in-memory cache list
        dependenciesInStorage.add(d);
        return false;
      }
    } catch (IOException e) {
      throw new ProjectManagerException("Unable to check for existence of dependency in storage: "
          + d.getFile(), e);
    }

    // We couldn't find this dependency in storage, it must be downloaded from artifactory
    return true;
  }

  public void persistDependency(final StartupDependencyDetails d, final String cacheKey, final File file) {

  }
}
