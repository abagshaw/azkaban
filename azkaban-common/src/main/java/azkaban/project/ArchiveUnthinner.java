package azkaban.project;

import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidatorConfigs;
import azkaban.project.validator.ValidatorManager;
import azkaban.project.validator.XmlValidatorManager;
import azkaban.spi.Storage;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static azkaban.utils.ThinArchiveUtils.*;


public class ArchiveUnthinner {
  private static final Logger log = LoggerFactory.getLogger(ArchiveUnthinner.class);

  private final Storage storage;

  private final Set<StartupDependency> dependenciesOnHDFS = ConcurrentHashMap.newKeySet();;

  @Inject
  public ArchiveUnthinner(final Storage storage) {
    this.storage = storage;
  }

  public Map<String, ValidationReport> validateProjectAndPersistDependencies(final Project project,
      final File archive, final File projectFolder, final File startupDependencies, final Props prop)
      throws ProjectManagerException {

    List<StartupDependency> dependencies;
    try {
      dependencies = parseStartupDependencies(startupDependencies);
    } catch (IOException e) {
      throw new ProjectManagerException("Unable to open or parse startup-dependencies.json", e);
    }

    // Filter the dependencies down to only the new dependencies that need downloading
    final List<StartupDependency> newDependencies =
        dependencies.stream().filter(this::mustDownloadDependency).collect(Collectors.toList());

    // Download the new dependencies
    final List<File> downloadedDependencyFiles =
        newDependencies.stream().map(d -> downloadDependency(projectFolder, d)).collect(Collectors.toList());

    return runValidator(project, archive, projectFolder, prop);
  }

  private Map<String, ValidationReport> runValidator(final Project project,
      final File archive, final File folder, final Props prop) {
    prop.put(ValidatorConfigs.PROJECT_ARCHIVE_FILE_PATH,
        archive.getAbsolutePath());
    // Basically, we want to make sure that for different invocations to the
    // uploadProject method,
    // the validators are using different values for the
    // PROJECT_ARCHIVE_FILE_PATH configuration key.
    // In addition, we want to reload the validator objects for each upload, so
    // that we can change the validator configuration files without having to
    // restart Azkaban web server. If the XmlValidatorManager is an instance
    // variable, 2 consecutive invocations to the uploadProject
    // method might cause the second one to overwrite the
    // PROJECT_ARCHIVE_FILE_PATH configuration parameter
    // of the first, thus causing a wrong archive file path to be passed to the
    // validators. Creating a separate XmlValidatorManager object for each
    // upload will prevent this issue without having to add
    // synchronization between uploads. Since we're already reloading the XML
    // config file and creating validator objects for each upload, this does
    // not add too much additional overhead.
    final ValidatorManager validatorManager = new XmlValidatorManager(prop);
    log.info("Validating project " + archive.getName()
        + " using the registered validators "
        + validatorManager.getValidatorsInfo().toString());
    return validatorManager.validate(project, folder);
  }

  private boolean mustDownloadDependency(final StartupDependency d) {
    // See if our in-memory cache of dependencies in storage already has this dependency listed
    // If it does, no need to download! It must already be in storage.
    if (dependenciesOnHDFS.contains(d)) return false;

    // Check if the dependency exists in storage
    if (this.storage.existsDependency(d.file, d.sha1)) {
      // It does, so we need to update our in-memory cache list
      dependenciesOnHDFS.add(d);
      return false;
    }

    // We couldn't find this dependency, so we must download it locally, validate it
    // then persist it in storage.
    return true;
  }

  private File downloadDependency(final File projectFolder, final StartupDependency d) {
    File downloadedJar = new File(projectFolder, d.destination + File.separator + d.file);
    FileChannel writeChannel;
    try {
      downloadedJar.createNewFile();
      FileOutputStream fileOS = new FileOutputStream(downloadedJar);
      writeChannel = fileOS.getChannel();
    } catch (IOException e) {
      throw new ProjectManagerException(String.format("In preparation for downloading, failed to create empty file %s", downloadedJar.getAbsolutePath()), e);
    }

    try {
      String toDownload = getArtifactoryUrlForDependency(d);
      ReadableByteChannel readChannel = Channels.newChannel(new URL(toDownload).openStream());
      writeChannel.transferFrom(readChannel, 0, Long.MAX_VALUE);
    } catch (Exception e) {
      throw new ProjectManagerException(String.format("Error while downloading dependency %s", d.file), e);
    }

    return downloadedJar;
  }
}
