package azkaban.project;

import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidatorConfigs;
import azkaban.project.validator.ValidatorManager;
import azkaban.project.validator.XmlValidatorManager;
import azkaban.spi.Storage;
import azkaban.utils.HashNotMatchException;
import azkaban.utils.HashUtils;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.collections.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static azkaban.utils.ThinArchiveUtils.*;


public class ArchiveUnthinner {
  private static final int MAX_DEPENDENCY_DOWNLOAD_RETRIES = 1;
  private static final Logger log = LoggerFactory.getLogger(ArchiveUnthinner.class);

  private final Storage storage;

  private final Set<StartupDependencyDetails> dependenciesOnHDFS = ConcurrentHashMap.newKeySet();

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
  public ArchiveUnthinner(final Storage storage) {
    this.storage = storage;
  }

  public Map<String, ValidationReport> validateProjectAndPersistDependencies(final Project project,
      final File projectArchive, final File projectFolder, final File startupDependenciesFile, final Props prop)
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
      if (mustDownloadDependency(d)) {
        newDependencies.add(d);
      } else {
        existingDependencies.add(d);
      }
    }

    // Download the new dependencies
    final List<StartupDependencyFile> downloadedDependencyFiles =
        newDependencies.stream().map(d -> new StartupDependencyFile(downloadDependency(projectFolder, d), d))
            .collect(Collectors.toList());

    Map<String, ValidationReport> reports = runValidator(project, projectFolder, prop);
    List<StartupDependencyFile> unmodifiedDependencyFiles = new ArrayList<>();
    // Check if any files were deleted or modified in the bundle
    if (!reports.values().stream().noneMatch(r -> r.getBundleModified())) {
      // At least one file was deleted or modified in the bundle
      // We need to figure out which ones were modified and which weren't
      for (StartupDependencyFile f : downloadedDependencyFiles) {
        try {
          if (f.getFile().exists() && HashUtils.isSameHash(f.getDetails().getSHA1(), HashUtils.SHA1.getHash(f.getFile()))) {
            unmodifiedDependencyFiles.add(f);
          }
        } catch (Exception e) {
          throw new ProjectManagerException("Error while checking modified files during project validation.", e);
        }
      }

      // Get the final list of startup dependencies that will be downloadable from storage
      List<StartupDependencyDetails> finalDependencies =
          ListUtils.union(
              unmodifiedDependencyFiles.stream().map(StartupDependencyFile::getDetails).collect(Collectors.toList()),
              existingDependencies);
      // Write this list back to the startup-dependencies.json file
      try {
        writeStartupDependencies(startupDependenciesFile, finalDependencies);
      } catch (IOException e) {
        throw new ProjectManagerException("Error while writing new startup-dependencies.json", e);
      }
    } else {
      // No files were deleted or modified in the bundle, so all files are unmodified
      unmodifiedDependencyFiles = downloadedDependencyFiles;
    }

    // Loop through unmodified dependency files, persist them in storage, then delete them
    // from the project directory - they do not need to be included in the thin archive because
    // they can be downloaded from storage when needed.
    for (StartupDependencyFile f : unmodifiedDependencyFiles) {
      this.storage.putDependency(f.getFile(), f.getDetails().getFileName(), f.getDetails().getSHA1());
      f.getFile().delete();
    }

    return reports;
  }

  private Map<String, ValidationReport> runValidator(final Project project,
      final File projectFolder, final Props prop) {
    final ValidatorManager validatorManager = new XmlValidatorManager(prop);
    log.info("Validating project (with thinArchive) " + project.getName()
        + " using the registered validators "
        + validatorManager.getValidatorsInfo().toString());
    return validatorManager.validate(project, projectFolder);
  }

  private boolean mustDownloadDependency(final StartupDependencyDetails d) {
    // See if our in-memory cache of dependencies in storage already has this dependency listed
    // If it does, no need to download! It must already be in storage.
    if (dependenciesOnHDFS.contains(d)) return false;

    // Check if the dependency exists in storage
    try {
      if (this.storage.existsDependency(d.getFileName(), d.getSHA1())) {
        // It does, so we need to update our in-memory cache list
        dependenciesOnHDFS.add(d);
        return false;
      }
    } catch (IOException e) {
      throw new ProjectManagerException("Unable to check for existence of dependency in storage: "
          + d.getFileName(), e);
    }

    // We couldn't find this dependency in storage, it must be downloaded from artifactory
    return true;
  }

  private File downloadDependency(final File projectFolder, final StartupDependencyDetails d) {
    return downloadDependency(projectFolder, d, 0);
  }

  private File downloadDependency(final File projectFolder, final StartupDependencyDetails d, int retries) {
    File downloadedJar = new File(projectFolder, d.getDestination() + File.separator + d.getFileName());
    FileChannel writeChannel;
    try {
      downloadedJar.createNewFile();
      FileOutputStream fileOS = new FileOutputStream(downloadedJar, false);
      writeChannel = fileOS.getChannel();
    } catch (IOException e) {
      throw new ProjectManagerException("In preparation for downloading, failed to create destination file " +
              downloadedJar.getAbsolutePath(), e);
    }

    try {
      String toDownload = getArtifactoryUrlForDependency(d);
      ReadableByteChannel readChannel = Channels.newChannel(new URL(toDownload).openStream());
      writeChannel.transferFrom(readChannel, 0, Long.MAX_VALUE);
    } catch (IOException e) {
      if (retries < MAX_DEPENDENCY_DOWNLOAD_RETRIES) {
        return downloadDependency(projectFolder, d, retries + 1);
      }
      throw new ProjectManagerException("Error while downloading dependency " + d.getFileName(), e);
    }

    try {
      validateDependencyHash(downloadedJar, d);
    } catch (HashNotMatchException e) {
      if (retries < MAX_DEPENDENCY_DOWNLOAD_RETRIES) {
        return downloadDependency(projectFolder, d, retries + 1);
      }
      throw new ProjectManagerException(e.getMessage());
    }

    return downloadedJar;
  }
}
