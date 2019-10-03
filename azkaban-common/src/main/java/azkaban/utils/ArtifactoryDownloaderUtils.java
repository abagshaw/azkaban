package azkaban.utils;

import azkaban.project.ProjectManagerException;
import azkaban.project.StartupDependencyDetails;
import java.io.File;
import java.io.IOException;
import java.net.URL;

import static azkaban.utils.ThinArchiveUtils.*;


public class ArtifactoryDownloaderUtils {
  public static final int MAX_DEPENDENCY_DOWNLOAD_TRIES = 2;

  public static void downloadDependency(final File destination, final StartupDependencyDetails d)
      throws HashNotMatchException, IOException {
    downloadDependency(destination, d, 0);
  }

  public static void downloadDependency(final File destination, final StartupDependencyDetails d, int tries)
      throws HashNotMatchException, IOException {
    try {
      tries++;
      FileDownloaderUtils.downloadToFile(destination, getArtifactoryUrlForDependency(d));
    } catch (IOException e) {
      if (tries < MAX_DEPENDENCY_DOWNLOAD_TRIES) {
        downloadDependency(destination, d, tries);
        return;
      }
      throw e;
    }

    try {
      validateDependencyHash(destination, d);
    } catch (HashNotMatchException e) {
      if (tries < MAX_DEPENDENCY_DOWNLOAD_TRIES) {
        // downloadDependency will overwrite our destination file if attempted again
        downloadDependency(destination, d, tries);
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
