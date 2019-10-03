package azkaban.utils;

import azkaban.project.StartupDependencyDetails;
import java.io.File;
import java.io.IOException;

import static azkaban.utils.ThinArchiveUtils.*;

public class ArtifactoryDownloader {
  public static final int MAX_DEPENDENCY_DOWNLOAD_TRIES = 2;

  public void downloadDependency(final File destination, final StartupDependencyDetails d)
      throws HashNotMatchException, IOException {
    downloadDependencyNoCache(destination, d, 0);
  }

  private void downloadDependencyNoCache(final File destination, final StartupDependencyDetails d, int tries)
      throws HashNotMatchException, IOException {
    try {
      tries++;
      FileDownloaderUtils.downloadToFile(destination, getArtifactoryUrlForDependency(d));
    } catch (IOException e) {
      if (tries < MAX_DEPENDENCY_DOWNLOAD_TRIES) {
        downloadDependencyNoCache(destination, d, tries);
        return;
      }
      throw e;
    }

    try {
      validateDependencyHash(destination, d);
    } catch (HashNotMatchException e) {
      if (tries < MAX_DEPENDENCY_DOWNLOAD_TRIES) {
        // downloadDependency will overwrite our destination file if attempted again
        downloadDependencyNoCache(destination, d, tries);
        return;
      }
      throw e;
    }
  }


  private static String getArtifactoryUrlForDependency(StartupDependencyDetails d) {
    String[] coordinateParts = d.getIvyCoordinates().split(":");
    return "http://dev-artifactory.corp.linkedin.com:8081/artifactory/release/"
        + coordinateParts[0].replace(".", "/") + "/"
        + coordinateParts[1] + "/"
        + coordinateParts[2] + "/"
        + d.getFile();
  }
}
