package azkaban.project;

import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.FileStatus;
import azkaban.spi.FileValidationStatus;
import azkaban.spi.Storage;
import azkaban.utils.DependencyDownloader;
import azkaban.utils.FileIOUtils;
import azkaban.utils.HashNotMatchException;
import azkaban.utils.Props;
import azkaban.utils.ValidatorUtils;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static azkaban.utils.ThinArchiveUtils.*;


public class ArchiveUnthinner {
  private static final Logger log = LoggerFactory.getLogger(ArchiveUnthinner.class);

  private final JdbcDependencyManager jdbcDependencyManager;
  private final DependencyDownloader dependencyDownloader;
  private final Storage storage;
  private final ValidatorUtils validatorUtils;

  @Inject
  public ArchiveUnthinner(final ValidatorUtils validatorUtils, final JdbcDependencyManager jdbcDependencyManager,
      final DependencyDownloader dependencyDownloader, final Storage storage) {
    this.validatorUtils = validatorUtils;
    this.storage = storage;
    this.jdbcDependencyManager = jdbcDependencyManager;
    this.dependencyDownloader = dependencyDownloader;
  }

  public Map<String, ValidationReport> validateProjectAndPersistDependencies(final Project project,
      final File projectFolder, final File startupDependenciesFile, final Props additionalProps) {
    Set<Dependency> dependencies = getDependenciesFromSpec(startupDependenciesFile);

    String validationKey = this.validatorUtils.getCacheKey(project, projectFolder, additionalProps);

    // Find the cached validation status (or NEW if the dep isn't cached) for each dependency.
    Map<Dependency, FileValidationStatus> depsToValidationStatus = getValidationStatuses(dependencies, validationKey);
    // removedCachedDeps: dependencies that have been processed before and are blacklisted (so should be removed)
    Set<Dependency> removedCachedDeps = filterValidationStatus(depsToValidationStatus, FileValidationStatus.REMOVED);
    // validCachedDeps: dependencies that are in storage and already verified to be valid
    Set<Dependency> validCachedDeps = filterValidationStatus(depsToValidationStatus, FileValidationStatus.VALID);
    // newDeps: dependencies that are not in storage and need to be verified
    Set<Dependency> newDeps = filterValidationStatus(depsToValidationStatus, FileValidationStatus.NEW);

    // Download the new dependencies
    final Set<DependencyFile> downloadedDeps = downloadDependencyFiles(projectFolder, newDeps);

    // Validate the project
    Map<String, ValidationReport> reports = this.validatorUtils.validateProject(project, projectFolder, additionalProps);
    if (reports.values().stream().anyMatch(r -> r.getStatus() == ValidationStatus.ERROR)) {
      // No point continuing, this project has been rejected, so just return the validation report
      // and don't waste any more time.
      return reports;
    }

    // Find which dependencies were removed, modified or untouched by the validator
    // pathToDownloadedDeps is created for performance reasons to allow getDepsFromReports to run in O(n) time
    // instead of O(n^2).
    Map<String, DependencyFile> pathToDownloadedDeps = getPathToDepFileMap(downloadedDeps);
    Set<DependencyFile> removedDownloadedDeps =
        getDepsFromReports(reports, pathToDownloadedDeps, ValidationReport::getRemovedFiles);
    Set<DependencyFile> modifiedDownloadedDeps =
        getDepsFromReports(reports, pathToDownloadedDeps, ValidationReport::getModifiedFiles);
    Set<DependencyFile> untouchedDownloadedDeps =
        Sets.difference(downloadedDeps, Sets.union(removedDownloadedDeps, modifiedDownloadedDeps));

    // Persist the unmodified dependencies and get a list of dependencies that we are 100% sure were successfully
    // persisted. It's possible that we were unable to persist some because another process was also uploading it.
    // In that case we will still continue with the project upload, but NOT persist an entry in the DB for it in
    // case the other process fails to upload. If we did persist an entry in the DB and the other process failed to
    // upload to storage, we would be in BIG TROUBLE!! because the DB would indicate all is well and that file is persisted
    // but in reality it doesn't exist on storage, so any flows that depend on that dependency will fail, even if you
    // try to reupload them! So we don't want to do that.
    Set<DependencyFile> guaranteedPersistedDeps = persistUntouchedNewDependencies(untouchedDownloadedDeps);

    updateValidationStatuses(guaranteedPersistedDeps, removedDownloadedDeps, validationKey);

    // See if any downloaded deps were modified/removed OR if there are any cached removed dependencies
    if (untouchedDownloadedDeps.size() < downloadedDeps.size() || removedCachedDeps.size() > 0) {
      // Either one or more of the dependencies we downloaded was removed/modified during validation
      // OR there are cached removed dependencies. Either way we need to remove them from the
      // startup-dependencies.json file.

      // Get the final list of startup dependencies that will be downloadable from storage
      Set<Dependency> finalDeps = Sets.union(validCachedDeps, untouchedDownloadedDeps);
      rewriteStartupDependencies(startupDependenciesFile, finalDeps);
    }

    // Delete untouched downloaded dependencies from the project
    untouchedDownloadedDeps.stream().forEach(d -> d.getFile().delete());

    return reports;
  }

  private void rewriteStartupDependencies(File startupDependenciesFile, Set<Dependency> finalDependencies) {
    // Write this list back to the startup-dependencies.json file
    try {
      writeStartupDependencies(startupDependenciesFile, finalDependencies);
    } catch (IOException e) {
      throw new ProjectManagerException("Error while writing new startup-dependencies.json", e);
    }
  }
  private Set<Dependency> getDependenciesFromSpec(File startupDependenciesFile) {
    try {
      return parseStartupDependencies(startupDependenciesFile);
    } catch (IOException e) {
      throw new ProjectManagerException("Unable to open or parse startup-dependencies.json", e);
    }
  }

  private Set<DependencyFile> persistUntouchedNewDependencies(Set<DependencyFile> untouchedNewDependencies) {
    final Set<DependencyFile> guaranteedPersistedDeps = new HashSet<>();
    try {
      for (DependencyFile f : untouchedNewDependencies) {
        // If the file has a status of OPEN (it should never have a status of NON_EXISTANT) then we will not
        // add an entry in the DB because it's possible that the other process that is currently writing the
        // dependency fails and we want to ensure that the DB ONLY includes entries for verified dependencies
        // when they are GUARANTEED to be persisted to storage.
        FileStatus resultOfPersisting = this.storage.putDependency(f);
        if (resultOfPersisting == FileStatus.CLOSED) {
          // The dependency has a status of closed, so we are guaranteed that it persisted successfully to storage.
          guaranteedPersistedDeps.add(f);
        }
      }
    } catch (Exception e) {
      throw new ProjectManagerException("Error while persisting dependencies.", e);
    }

    return guaranteedPersistedDeps;
  }

  private Map<Dependency, FileValidationStatus> getValidationStatuses(Set<Dependency> deps,
      String validationKey) {
    try {
      return this.jdbcDependencyManager.getValidationStatuses(deps, validationKey);
    } catch (SQLException e) {
      throw new ProjectManagerException(
          String.format("Unable to query DB for validation statuses "
            + "for project with validationKey %s", validationKey));
    }
  }

  private void updateValidationStatuses(Set<? extends Dependency> guaranteedPersistedDeps,
      Set<? extends Dependency> removedDeps, String validationKey) {
    // guaranteedPersistedDeps are new dependencies that we have just validated and found to be VALID and are now
    // persisted to storage.

    // removedDeps are new dependencies that we have just validated and found to be REMOVED and are NOT persisted
    // to storage.
    Map<Dependency, FileValidationStatus> depValidationStatuses = new HashMap<>();
    guaranteedPersistedDeps.stream().forEach(d -> depValidationStatuses.put(d, FileValidationStatus.VALID));
    removedDeps.stream().forEach(d -> depValidationStatuses.put(d, FileValidationStatus.REMOVED));
    try {
      this.jdbcDependencyManager.updateValidationStatuses(depValidationStatuses, validationKey);
    } catch (SQLException e) {
      throw new ProjectManagerException(
          String.format("Unable to update DB for validation statuses "
              + "for project with validationKey %s", validationKey));
    }
  }

  private Set<DependencyFile> downloadDependencyFiles(File projectFolder,
      Set<Dependency> toDownload) {
    final Set<DependencyFile> downloadedFiles = new HashSet();
    for (Dependency d : toDownload) {
      File downloadedJar = new File(projectFolder, d.getDestination() + File.separator + d.getFileName());
      try {
        this.dependencyDownloader.downloadDependency(downloadedJar, d);
      } catch (IOException | HashNotMatchException e) {
        throw new ProjectManagerException("Error while downloading dependency " + d.getFileName(), e);
      }
      downloadedFiles.add(new DependencyFile(downloadedJar, d));
    }
    return downloadedFiles;
  }

  private Set<DependencyFile> getDepsFromReports(Map<String, ValidationReport> reports,
      Map<String, DependencyFile> pathToDep, Function<ValidationReport, Set<File>> fn) {
    return reports.values()
        .stream()
        .map(fn)
        .flatMap(set -> set.stream())
        .map(f -> pathToDep.get(FileIOUtils.getCanonicalPath(f)))
        .collect(Collectors.toSet());
  }

  private Map<String, DependencyFile> getPathToDepFileMap(Set<DependencyFile> depFiles) {
    return depFiles
        .stream()
        .collect(Collectors.toMap(d -> FileIOUtils.getCanonicalPath(d.getFile()), e -> e));
  }


  private Set<Dependency> filterValidationStatus(Map<Dependency, FileValidationStatus> validationStatuses,
      FileValidationStatus status) {
    return validationStatuses
        .keySet()
        .stream()
        .filter(d -> validationStatuses.get(d) == status)
        .collect(Collectors.toSet());
  }
}
