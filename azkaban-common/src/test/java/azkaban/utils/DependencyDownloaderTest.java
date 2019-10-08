package azkaban.utils;

import azkaban.test.executions.ThinArchiveTestSampleData;
import java.io.File;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static azkaban.Constants.ConfigurationKeys.*;
import static org.mockito.Mockito.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest(FileDownloaderUtils.class)
public class DependencyDownloaderTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  public final String DOWNLOAD_BASE_URL = "https://example.com/";
  public URL depAFullUrl;

  public DependencyDownloader dependencyDownloader;
  public Props props;

  @Before
  public void setup() throws Exception {
    this.props = new Props();
    this.props.put(AZKABAN_STARTUP_DEPENDENCIES_DOWNLOAD_BASE_URL, DOWNLOAD_BASE_URL);

    depAFullUrl = new URL(new URL(DOWNLOAD_BASE_URL), ThinArchiveTestSampleData.getDepAPath());

    dependencyDownloader = new DependencyDownloader(this.props);
  }

  @Test
  public void testDownloadDependencySuccess() throws Exception {
    PowerMockito.mockStatic(FileDownloaderUtils.class);
    // When FileDownloaderUtils.downloadToFile() is called, write the content to the file as if it was downloaded
    PowerMockito.doAnswer((Answer) invocation -> {
      File destFile = (File) invocation.getArguments()[0];
      FileUtils.writeStringToFile(destFile, ThinArchiveTestSampleData.getDepAContent());
      return null;
    }).when(FileDownloaderUtils.class, "downloadToFile", Mockito.any(File.class), Mockito.any(URL.class));

    File destinationFile = TEMP_DIR.newFile(ThinArchiveTestSampleData.getDepA().getFile());
    this.dependencyDownloader.downloadDependency(destinationFile, ThinArchiveTestSampleData.getDepA());

    PowerMockito.verifyStatic(FileDownloaderUtils.class, Mockito.times(1));
    FileDownloaderUtils.downloadToFile(destinationFile, depAFullUrl);
  }

  @Test
  public void testDownloadDependencyHashInvalidOneRetry() throws Exception {
    final AtomicInteger countCall = new AtomicInteger();

    PowerMockito.mockStatic(FileDownloaderUtils.class);
    // When FileDownloaderUtils.downloadToFile() is called, write the content to the file as if it was downloaded
    PowerMockito.doAnswer((Answer) invocation -> {
      File destFile = (File) invocation.getArguments()[0];

      if (countCall.getAndIncrement() == 0) {
        // On the first call, write the wrong content to trigger a hash mismatch
        FileUtils.writeStringToFile(destFile, "WRONG CONTENT!!!!!!!");
      } else {
        FileUtils.writeStringToFile(destFile, ThinArchiveTestSampleData.getDepAContent());
      }
      return null;
    }).when(FileDownloaderUtils.class, "downloadToFile", Mockito.any(File.class), Mockito.any(URL.class));

    File destinationFile = TEMP_DIR.newFile(ThinArchiveTestSampleData.getDepA().getFile());
    this.dependencyDownloader.downloadDependency(destinationFile, ThinArchiveTestSampleData.getDepA());

    PowerMockito.verifyStatic(FileDownloaderUtils.class, Mockito.times(2));
    FileDownloaderUtils.downloadToFile(destinationFile, depAFullUrl);
  }

  @Test
  public void testDownloadDependencyHashInvalidRetryExceededFail() throws Exception {
    PowerMockito.mockStatic(FileDownloaderUtils.class);
    // When FileDownloaderUtils.downloadToFile() is called, write the content to the file as if it was downloaded
    PowerMockito.doAnswer((Answer) invocation -> {
      File destFile = (File) invocation.getArguments()[0];
      FileUtils.writeStringToFile(destFile, "WRONG CONTENT!!!!!!!");
      return null;
    }).when(FileDownloaderUtils.class, "downloadToFile", Mockito.any(File.class), Mockito.any(URL.class));

    File destinationFile = TEMP_DIR.newFile(ThinArchiveTestSampleData.getDepA().getFile());

    boolean hitException = false;
    try {
      this.dependencyDownloader.downloadDependency(destinationFile, ThinArchiveTestSampleData.getDepA());
    } catch (HashNotMatchException e) {
      // Good! We wanted this exception.
      hitException = true;
    }

    if (!hitException) {
      Assert.fail("Expected HashNotMatchException but didn't get any.");
    }

    // We expect the download to be attempted the maximum number of times before it fails
    PowerMockito.verifyStatic(FileDownloaderUtils.class,
        Mockito.times(DependencyDownloader.MAX_DEPENDENCY_DOWNLOAD_TRIES));
    FileDownloaderUtils.downloadToFile(destinationFile, depAFullUrl);
  }
}
