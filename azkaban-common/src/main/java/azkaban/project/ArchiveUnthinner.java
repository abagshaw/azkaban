package azkaban.project;

import azkaban.project.validator.ValidationReport;
import azkaban.spi.StartupDependencyDetails;
import azkaban.spi.Storage;
import azkaban.utils.DependencyDownloader;
import azkaban.utils.DependencyStorage;
import azkaban.utils.HashNotMatchException;
import azkaban.utils.ValidatorUtils;
import java.io.File;
import java.io.IOException;
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
      final File projectFolder, final File startupDependenciesFile)
      throws ProjectManagerException {

    List<StartupDependencyDetails> dependencies;
    try {
      dependencies = parseStartupDependencies(startupDependenciesFile);
    } catch (IOException e) {
      throw new ProjectManagerException("Unable to open or parse startup-dependencies.json", e);
    }

    // existingDependencies: dependencies that are already in storage and verified
    List<StartupDependencyDetails> existingDependencies = new ArrayList<>();
    // newDependencies: dependencies that are not in storage and need to be verified
    List<StartupDependencyDetails> newDependencies = new ArrayList<>();

    for (StartupDependencyDetails d : dependencies) {
      if (!this.dependencyStorage.dependencyExistsAndIsValidated(d)) {
        newDependencies.add(d);
      } else {
        existingDependencies.add(d);
      }
    }

    // Download the new dependencies
    final List<StartupDependencyFile> downloadedDependencies = downloadDependencyFiles(projectFolder, newDependencies);

    Map<String, ValidationReport> reports = this.validatorUtils.validateProject(project, projectFolder);
    List<StartupDependencyFile> unmodifiedDependencies = new ArrayList<>();

    // A set of filename strings representing jars that have been either modified or deleted during
    // the execution of one or more validators.
    Set<String> modifiedFiles = reports.values()
        .stream()
        .map(r -> r.getModifiedFiles())
        .flatMap(set -> set.stream())
        .collect(Collectors.toSet());

    // Check if any files were deleted or modified in the bundle
    if (!modifiedFiles.isEmpty()) {
      // At least one file was deleted or modified in the bundle
      // We need to figure out which ones were modified and which weren't
      for (StartupDependencyFile f : downloadedDependencies) {
        try {
          if (f.getFile().exists() && !modifiedFiles.contains(f.getDetails().getFile())) {
            unmodifiedDependencies.add(f);
          }
        } catch (Exception e) {
          throw new ProjectManagerException("Error while checking modified files during project validation.", e);
        }
      }

      // Get the final list of startup dependencies that will be downloadable from storage
      List<StartupDependencyDetails> finalDependencies = new ArrayList<>();
      finalDependencies.addAll(existingDependencies);
      finalDependencies.addAll(
          unmodifiedDependencies.stream().map(StartupDependencyFile::getDetails).collect(Collectors.toList()));
      // Write this list back to the startup-dependencies.json file
      try {
        writeStartupDependencies(startupDependenciesFile, finalDependencies);
      } catch (IOException e) {
        throw new ProjectManagerException("Error while writing new startup-dependencies.json", e);
      }
    } else {
      // No files were deleted or modified in the bundle, so all files are unmodified
      unmodifiedDependencies = downloadedDependencies;
    }

    // Loop through unmodified dependency files, persist them in storage, add them to in-memory cache
    // then delete them from the project directory - they do not need to be included in the thin archive
    // because they can be downloaded from storage when needed.
    for (StartupDependencyFile f : unmodifiedDependencies) {
      try {
        this.dependencyStorage.persistDependency(f);
      } catch (Exception e) {
        throw new ProjectManagerException(e.getMessage(), e);
      }
      f.getFile().delete();
    }

    return reports;
  }

  private List<StartupDependencyFile> downloadDependencyFiles(File projectFolder, List<StartupDependencyDetails> toDownload) {
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
