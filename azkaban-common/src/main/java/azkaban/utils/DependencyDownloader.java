package azkaban.utils;

import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.DownloadOrigin;
import azkaban.spi.Storage;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import javax.inject.Inject;
import org.apache.commons.io.IOUtils;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_STARTUP_DEPENDENCIES_REMOTE_DOWNLOAD_BASE_URL;
import static azkaban.utils.ThinArchiveUtils.*;

public class DependencyDownloader {
  public static final int MAX_DEPENDENCY_DOWNLOAD_TRIES = 2;

  private final Props props;
  private final Storage storage;

  @Inject
  public DependencyDownloader(Props props, Storage storage) {
    this.props = props;
    this.storage = storage;
  }

  public void downloadDependency(final DependencyFile f, DownloadOrigin origin)
      throws HashNotMatchException, IOException {
    downloadDependency(f, origin, 0);
  }

  private void downloadDependency(final DependencyFile f, DownloadOrigin origin, int tries)
      throws HashNotMatchException, IOException {
    try {
      tries++;
      if (origin == DownloadOrigin.REMOTE) {
        FileDownloaderUtils.downloadToFile(f.getFile(), getUrlForDependency(f));
      } else if (origin == DownloadOrigin.STORAGE) {
        FileOutputStream fos = new FileOutputStream(f.getFile());
        IOUtils.copy(this.storage.getDependency(f), fos);
      } else {
        throw new RuntimeException("Unrecognized origin type for dependency download: " + origin.toString());
      }
    } catch (IOException e) {
      if (tries < MAX_DEPENDENCY_DOWNLOAD_TRIES) {
        downloadDependency(f, origin, tries);
        return;
      }
      throw e;
    }

    try {
      validateDependencyHash(f);
    } catch (HashNotMatchException e) {
      if (tries < MAX_DEPENDENCY_DOWNLOAD_TRIES) {
        // downloadDependency will overwrite our destination file if attempted again
        downloadDependency(f, origin, tries);
        return;
      }
      throw e;
    }
  }


  private URL getUrlForDependency(Dependency d) throws MalformedURLException {
    return new URL(
        new URL(this.props.getString(AZKABAN_STARTUP_DEPENDENCIES_REMOTE_DOWNLOAD_BASE_URL)),
        convertIvyCoordinateToPath(d));
  }
}
