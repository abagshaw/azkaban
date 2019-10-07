package azkaban.utils;

import azkaban.spi.StartupDependencyDetails;
import java.io.File;
import java.io.IOException;
import javax.inject.Inject;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_STARTUP_DEPENDENCIES_DOWNLOAD_BASE_URL;
import static azkaban.utils.ThinArchiveUtils.*;

public class DependencyDownloader {
  public static final int MAX_DEPENDENCY_DOWNLOAD_TRIES = 2;

  private final Props props;

  @Inject
  public DependencyDownloader(Props props) {
    this.props = props;
  }

  public void downloadDependency(final File destination, final StartupDependencyDetails d)
      throws HashNotMatchException, IOException {
    downloadDependency(destination, d, 0);
  }

  private void downloadDependency(final File destination, final StartupDependencyDetails d, int tries)
      throws HashNotMatchException, IOException {
    try {
      tries++;
      FileDownloaderUtils.downloadToFile(destination, getUrlForDependency(d));
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


  private String getUrlForDependency(StartupDependencyDetails d) {
    return this.props.getString(AZKABAN_STARTUP_DEPENDENCIES_DOWNLOAD_BASE_URL) + convertIvyCoordinateToPath(d);
  }
}
