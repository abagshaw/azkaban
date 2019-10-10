package azkaban.project;

import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.FileValidationStatus;
import azkaban.utils.DependencyDownloader;
import azkaban.utils.DependencyManager;
import azkaban.utils.HashNotMatchException;
import azkaban.utils.Props;
import azkaban.utils.ValidatorUtils;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static azkaban.utils.ThinArchiveUtils.*;


public class ArchiveUnthinner {
  private static final Logger log = LoggerFactory.getLogger(ArchiveUnthinner.class);

  private final DependencyManager dependencyManager;
  private final DependencyDownloader dependencyDownloader;

  private final ValidatorUtils validatorUtils;

  @Inject
  public ArchiveUnthinner(final ValidatorUtils validatorUtils, final DependencyManager dependencyManager,
      final DependencyDownloader dependencyDownloader) {
    this.validatorUtils = validatorUtils;
    this.dependencyManager = dependencyManager;
    this.dependencyDownloader = dependencyDownloader;
  }

  public Map<String, ValidationReport> validateProjectAndPersistDependencies(final Project project,
      final File projectFolder, final File startupDependenciesFile, final Props additionalProps) {
    Set<Dependency> dependencies = getDependenciesFromSpec(startupDependenciesFile);

    String validatorKey = this.validatorUtils.getCacheKey(project, projectFolder, additionalProps);

    Map<Dependency, FileValidationStatus> depsToValidationStatus = getValidationStatuses(dependencies, validatorKey);
    // removedDependencies: dependencies that have been processed before and are blacklisted (so should be removed)
    Set<Dependency> removedDependencies = filterValidationStatus(depsToValidationStatus, FileValidationStatus.REMOVED);
    // newDependencies: dependencies that are not in storage and need to be verified
    Set<Dependency> newDependencies = filterValidationStatus(depsToValidationStatus, FileValidationStatus.NEW);
    // validDependencies: dependencies that are in storage and already verified to be valid
    Set<Dependency> validDependencies = filterValidationStatus(depsToValidationStatus, FileValidationStatus.VALID);

    // Download the new dependencies
    final Set<DependencyFile> downloadedDeps = downloadDependencyFiles(projectFolder, newDependencies);

    // Validate the project
    Map<String, ValidationReport> reports = this.validatorUtils.validateProject(project, projectFolder, additionalProps);
    if (reports.values().stream().anyMatch(r -> r.getStatus() == ValidationStatus.ERROR)) {
      // No point continuing, this project has been rejected, so just return the validation report
      // and don't waste any more time.
      return reports;
    }

    // Find which dependencies were unmodified (touchedFiles is a subset of downloadedDeps)
    // so the difference between then will be the files that were untouched (neither modified nor removed).
    Map<String, DependencyFile> pathToDownloadedDependencies = getPathMap(downloadedDeps);
    Set<DependencyFile> removedFiles = getRemovedFiles(reports, pathToDownloadedDependencies);
    Set<DependencyFile> modifiedFiles = getModifiedFiles(reports, pathToDownloadedDependencies);
    Set<DependencyFile> untouchedNewDependencies =
        Sets.difference(downloadedDeps, Sets.union(removedFiles, modifiedFiles));

    // Persist the unmodified dependencies and get a list of dependencies that we are 100% sure were successfully
    // persisted. It's possible that we were unable to persist some because another process was also uploading it.
    // In that case we will still continue with the project upload, but NOT persist an entry in the DB for it in
    // case the other process fails to upload. If we did persist an entry in the DB and the other process failed to
    // upload to storage, we would be in BIG TROUBLE!! because the DB would indicate all is well and that file is persisted
    // but in reality it doesn't exist on storage, so any flows that depend on that dependency will fail, even if you
    // try to reupload them! So we don't want to do that.
    Set<DependencyFile> guaranteedPersistedDeps = persistUntouchedNewDependencies(untouchedNewDependencies);

    updateValidationStatuses(guaranteedPersistedDeps, removedFiles);

    // See if all downloaded dependencies were unmodified
    if (untouchedNewDependencies.size() < downloadedDeps.size()) {
      // There were some modified or deleted dependencies,
      // so we need to remove them from the startup-dependencies.json file.
      rewriteStartupDependencies(startupDependenciesFile, untouchedNewDependencies, existingDependencies);
    }

    // Delete untouched new dependencies from the project
    untouchedNewDependencies.stream().forEach(d -> d.getFile().delete());

    return reports;
  }

  private List<DependencyFile> getUntouchedNewDependencies(Set<DependencyFile> downloadedDependencies,
      Set<File> modifiedFiles, Set<File> removedFiles) {
    return downloadedDependencies
        .stream()
        .filter(d -> !modifiedFiles.contains(d.getFile()) && !removedFiles.contains(d.getFile()))
        .collect(Collectors.toList());
  }

  private void rewriteStartupDependencies(File startupDependenciesFile,
      Set<DependencyFile> untouchedNewDependencyFiles, Set<Dependency> existingDependencies) {
    // Get the final list of startup dependencies that will be downloadable from storage
    Set<Dependency> finalDependencies =
        Sets.union(existingDependencies, mapDepFilesToDetails(untouchedNewDependencyFiles));

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

  private Set<File> getModifiedFiles(Map<String, ValidationReport> reports) {
    return reports.values()
        .stream()
        .map(r -> r.getModifiedFiles())
        .flatMap(set -> set.stream())
        .collect(Collectors.toSet());
  }

  private Set<File> getRemovedFiles(Map<String, ValidationReport> reports) {
    return reports.values()
        .stream()
        .map(r -> r.getRemovedFiles())
        .flatMap(set -> set.stream())
        .collect(Collectors.toSet());
  }

  private Map<String, DependencyFile> getPathToDepFileMap(Map<String, ValidationReport> reports,
      Set<DependencyFile> depFiles) {
    for (DependencyFile d : depFiles) {
      hashToDep.put(d.getSHA1(), d);
    }

    return reports.values()
        .stream()
        .map(r -> r.getRemovedFiles())
        .flatMap(set -> set.stream())
        .collect(Collectors.toSet());
  }

  private Set<DependencyFile> persistUntouchedNewDependencies(Set<DependencyFile> untouchedNewDependencies) {
    try {
      this.dependencyManager.persistDependencies(untouchedNewDependencies);
    } catch (Exception e) {
      throw new ProjectManagerException("Error while persisting dependencies.", e);
    }
  }

  private Map<Dependency, FileValidationStatus> getValidationStatuses(Set<Dependency> deps,
      String validatorKey) {
    try {
      return this.dependencyManager.getValidationStatuses(deps, validatorKey);
    } catch (SQLException e) {
      throw new ProjectManagerException(
          String.format("Unable to query DB for validation statuses "
            + "for project with validatorKey %s", validatorKey));
    }
  }

  private void updateValidationStatuses(Set<Dependency> guaranteedPersistedDeps,
      Set<Dependency> removedDeps, String validatorKey) {
    // guaranteedPersistedDeps are new dependencies that we have just validated and found to be VALID and are now
    // persisted to storage.

    // removedDeps are new dependencies that we have just validated and found to be REMOVED and are NOT persisted
    // to storage.
    Map<Dependency, FileValidationStatus> depValidationStatuses = new HashMap<>();
    guaranteedPersistedDeps.stream().forEach(d -> depValidationStatuses.put(d, FileValidationStatus.VALID));
    removedDeps.stream().forEach(d -> depValidationStatuses.put(d, FileValidationStatus.REMOVED));
    try {
      this.dependencyManager.updateValidationStatuses(depValidationStatuses, validatorKey);
    } catch (SQLException e) {
      throw new ProjectManagerException(
          String.format("Unable to update DB for validation statuses "
              + "for project with validatorKey %s", validatorKey));
    }
  }

  private Set<Dependency> filterValidationStatus(Map<Dependency, FileValidationStatus> validationStatuses,
      FileValidationStatus status) {
    return validationStatuses
        .keySet()
        .stream()
        .filter(d -> validationStatuses.get(d) == status)
        .collect(Collectors.toSet());
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
}
