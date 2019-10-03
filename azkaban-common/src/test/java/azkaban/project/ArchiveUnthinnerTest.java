/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package azkaban.project;

import azkaban.spi.Storage;
import azkaban.test.executions.ThinArchiveTestSampleData;
import azkaban.utils.ArtifactoryDownloader;
import azkaban.utils.FileDownloaderUtils;
import azkaban.utils.ThinArchiveUtils;
import azkaban.utils.ValidatorUtils;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
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

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ArtifactoryDownloader.class)
public class ArchiveUnthinnerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private final Project project = new Project(107, "Test_Project");
  private File projectFolder;

  private ArchiveUnthinner archiveUnthinner;
  private ValidatorUtils validatorUtils;
  private Storage storage;

  @Before
  public void setUp() throws Exception {
    this.storage = mock(Storage.class);
    this.validatorUtils = mock(ValidatorUtils.class);
    this.archiveUnthinner = spy(new ArchiveUnthinner(this.storage, this.validatorUtils));

    // Create test project directory
    // ../
    // ../lib/some-snapshot.jar
    // ../app-meta/startup-dependencies.json
    projectFolder = TEMP_DIR.newFolder("testproj");
    File libFolder = new File(projectFolder, "lib");
    libFolder.mkdirs();
    File appMetaFolder = new File(projectFolder, "app-meta");
    libFolder.mkdirs();
    FileUtils.writeStringToFile(new File(libFolder, "some-snapshot.jar"), "oldcontent");
    FileUtils.writeStringToFile(new File(appMetaFolder, "startup-dependencies.json"),
        ThinArchiveTestSampleData.getRawJSON());
  }

  @Test
  public void freshValidProject() throws Exception {
    PowerMockito.mockStatic(FileDownloaderUtils.class);

    StartupDependencyDetails depA = ThinArchiveTestSampleData.getDepA();
    StartupDependencyDetails depB = ThinArchiveTestSampleData.getDepB();
    File depAInArtifactory = TEMP_DIR.newFile(depA.getFile());
    File depBInArtifactory = TEMP_DIR.newFile(depB.getFile());
    FileUtils.writeStringToFile(depAInArtifactory, ThinArchiveTestSampleData.getDepAContent());
    FileUtils.writeStringToFile(depBInArtifactory, ThinArchiveTestSampleData.getDepBContent());

    // Indicate that the dependencies are not in storage, forcing them to be downloaded from artifactory
    when(this.storage.existsDependency(depA.getFile(), depA.getSHA1())).thenReturn(false);
    when(this.storage.existsDependency(depB.getFile(), depB.getSHA1())).thenReturn(false);

    // When ArtifactoryDownloaderUtils.downloadDependency() is called,
    // write the content to the file as if it was downloaded
    PowerMockito.doAnswer((Answer) invocation -> {
      File destFile = (File) invocation.getArguments()[0];
      StartupDependencyDetails requestDependency = (StartupDependencyDetails) invocation.getArguments()[1];

      String contentToWrite = requestDependency.equals(depA) ?
          ThinArchiveTestSampleData.getDepAContent() :
          ThinArchiveTestSampleData.getDepBContent();

      FileUtils.writeStringToFile(destFile, contentToWrite);
      return null;
    }).when(ArtifactoryDownloader.class, "downloadDependency",
        Mockito.any(File.class), Mockito.any(StartupDependencyDetails.class));

    // When the unthinner attempts to validate the project, return an empty map (indicating that the
    // validator found no errors and made no changes to the project)
    when(this.validatorUtils.validateProject(this.project, this.projectFolder)).thenReturn(new HashMap<>());

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    this.archiveUnthinner.validateProjectAndPersistDependencies(this.project, this.projectFolder,
        startupDependenciesFile);
  }

  @Test
  public void otherTest() throws IOException {
    /*when(this.storage.getDependency(depA.getFile(), depA.getSHA1()))
        .thenReturn(new FileInputStream(depAInStorage));
    when(this.storage.getDependency(depB.getFile(), depB.getSHA1()))
        .thenReturn(new FileInputStream(depBInStorage));*/
  }
}
