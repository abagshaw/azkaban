package azkaban.project;

import azkaban.project.validator.ValidationReport;
import azkaban.spi.Storage;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileOutputStream;
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
      final File archive, final File folder, final File startupDependencies, final Props prop)
      throws ProjectManagerException {

    try {
      final List<StartupDependency> dependencies = parseStartupDependencies(startupDependencies);

      // Download the files from artifactory
      final List<StartupDependency> newDependencies =
          dependencies.stream().filter(this::mustDownloadDependency).collect(Collectors.toList());

      for (StartupDependency d : dependencies) {
        ensureDependencyPersisted(d);
        if (dependenciesOnHDFS.contains(d)) {
          continue;
        }

        File downloadedJar = new File(folder, d.destination + File.separator + d.file);

        String toDownload = getArtifactoryUrlForDependency(d);

        ReadableByteChannel readChannel = Channels.newChannel(new URL(toDownload).openStream());
        FileOutputStream fileOS = new FileOutputStream(downloadedJar);
        FileChannel writeChannel = fileOS.getChannel();
        writeChannel.transferFrom(readChannel, 0, Long.MAX_VALUE);

        this.storage.putDependency(downloadedJar, d.file, d.sha1);
      }


    } catch (Exception e) {
      throw new ProjectManagerException("Unable to open or parse startup-dependencies.json", e);
    }

    return validateProject(project, archive, folder, prop);
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

    // We couldn't find this dependency, so we must download it.
    return true;
  }
}
