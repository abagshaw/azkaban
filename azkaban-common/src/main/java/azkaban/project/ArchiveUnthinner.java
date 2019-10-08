package azkaban.project;

import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.spi.StartupDependencyDetails;
import azkaban.utils.DependencyDownloader;
import azkaban.utils.DependencyStorage;
import azkaban.utils.HashNotMatchException;
import azkaban.utils.Props;
import azkaban.utils.ValidatorUtils;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
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

  private final DependencyStorage dependencyStorage;
  private final DependencyDownloader dependencyDownloader;

  private final ValidatorUtils validatorUtils;

  private static class StartupDependencyFile {
    private final File file;
    private final StartupDependencyDetails details;

    public StartupDependencyFile(File f, StartupDependencyDetails sd) {
      this.file = f;
      this.details = sd;
    }

    public File getFile() { return file; }
    public StartupDependencyDetails getDetails() { return details; }
  }

  @Inject
  public ArchiveUnthinner(final ValidatorUtils validatorUtils, final DependencyStorage dependencyStorage,
      final DependencyDownloader dependencyDownloader) {
    this.validatorUtils = validatorUtils;
    this.dependencyStorage = dependencyStorage;
    this.dependencyDownloader = dependencyDownloader;
  }

  public Map<String, ValidationReport> validateProjectAndPersistDependencies(final Project project,
      final File projectFolder, final File startupDependenciesFile, final Props additionalProps) {
    List<StartupDependencyDetails> dependencies = getDependenciesList(startupDependenciesFile);

    String validatorKey = this.validatorUtils.getCacheKey(project, projectFolder, additionalProps);

    // existingDependencies: dependencies that are already in storage and verified
    List<StartupDependencyDetails> existingDependencies = new ArrayList<>();
    // newDependencies: dependencies that are not in storage and need to be verified
    List<StartupDependencyDetails> newDependencies = new ArrayList<>();
    for (StartupDependencyDetails d : dependencies) {
      if (isExistingDependency(d, validatorKey)) {
        existingDependencies.add(d);
      } else {
        newDependencies.add(d);
      }
    }

    // Download the new dependencies
    final List<StartupDependencyFile> downloadedDependencies = downloadDependencyFiles(projectFolder, newDependencies);

    // Validate the project
    Map<String, ValidationReport> reports = this.validatorUtils.validateProject(project, projectFolder, additionalProps);
    if (reports.values().stream().anyMatch(r -> r.getStatus() == ValidationStatus.ERROR)) {
      // No point continuing, this project has been rejected, so just return the validation report
      // and don't waste any more time.
      return reports;
    }

    // Find which dependencies were unmodified
    Set<File> modifiedFiles = getModifiedFiles(reports);
    Set<File> removedFiles = getRemovedFiles(reports);
    List<StartupDependencyFile> untouchedNewDependencies =
        getUntouchedNewDependencies(downloadedDependencies, modifiedFiles, removedFiles);

    // Persist the unmodified dependencies
    persistUntouchedNewDependencies(untouchedNewDependencies, validatorKey);

    // See if all downloaded dependencies were unmodified
    if (untouchedNewDependencies.size() < downloadedDependencies.size()) {
      // There were some modified dependencies, so we need to remove them from the startup-dependencies.json file.
      rewriteStartupDependencies(startupDependenciesFile, untouchedNewDependencies, existingDependencies);
    }

    // Delete untouched new dependencies from the project
    untouchedNewDependencies.stream().forEach(d -> d.getFile().delete());

    return reports;
  }

  private List<StartupDependencyFile> getUntouchedNewDependencies(List<StartupDependencyFile> downloadedDependencies,
      Set<File> modifiedFiles, Set<File> removedFiles) {
    return downloadedDependencies
        .stream()
        .filter(d -> !modifiedFiles.contains(d.getFile()) && !removedFiles.contains(d.getFile()))
        .collect(Collectors.toList());
  }

  private void rewriteStartupDependencies(File startupDependenciesFile,
      List<StartupDependencyFile> untouchedNewDependencies, List<StartupDependencyDetails> existingDependencies) {
    // Get the final list of startup dependencies that will be downloadable from storage
    List<StartupDependencyDetails> finalDependencies = new ArrayList<>();
    finalDependencies.addAll(existingDependencies);
    finalDependencies.addAll(
        untouchedNewDependencies.stream().map(StartupDependencyFile::getDetails).collect(Collectors.toList()));

    // Write this list back to the startup-dependencies.json file
    try {
      writeStartupDependencies(startupDependenciesFile, finalDependencies);
    } catch (IOException e) {
      throw new ProjectManagerException("Error while writing new startup-dependencies.json", e);
    }
  }
  private List<StartupDependencyDetails> getDependenciesList(File startupDependenciesFile) {
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

  private void persistUntouchedNewDependencies(List<StartupDependencyFile> untouchedNewDependencies,
      String validatorKey) {
    // Loop through new untouched dependency files, persist them in storage, add them to in-memory cache
    // then delete them from the project directory - they do not need to be included in the thin archive
    // because they can be downloaded from storage when needed.
    for (StartupDependencyFile f : untouchedNewDependencies) {
      try {
        this.dependencyStorage.persistDependency(f.getFile(), f.getDetails(), validatorKey);
      } catch (Exception e) {
        throw new ProjectManagerException("Error while persisting dependency " + f.getDetails().getFile(), e);
      }
    }
  }

  private boolean isExistingDependency(StartupDependencyDetails d, String validatorKey) {
    try {
      return this.dependencyStorage.dependencyExistsAndIsValidated(d, validatorKey);
    } catch (SQLException e) {
      throw new ProjectManagerException(
          String.format("Unable to query DB to see if dependency exists in storage and is validated."
                  + "Dependency Name: %s Dependency Hash: %s Validator Cache Key: %s",
              d.getFile(), d.getSHA1(), validatorKey), e);
    }
  }

  private List<StartupDependencyFile> downloadDependencyFiles(File projectFolder,
      List<StartupDependencyDetails> toDownload) {
    final List<StartupDependencyFile> downloadedFiles = new ArrayList();
    for (StartupDependencyDetails d : toDownload) {
      File downloadedJar = new File(projectFolder, d.getDestination() + File.separator + d.getFile());
      try {
        this.dependencyDownloader.downloadDependency(downloadedJar, d);
      } catch (IOException | HashNotMatchException e) {
        throw new ProjectManagerException("Error while downloading dependency " + d.getFile(), e);
      }
      downloadedFiles.add(new StartupDependencyFile(downloadedJar, d));
    }
    return downloadedFiles;
  }
}
