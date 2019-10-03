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
import azkaban.utils.ThinArchiveUtils;
import azkaban.utils.ValidatorUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.mockito.Mockito.*;

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
    this.archiveUnthinner = new ArchiveUnthinner(this.storage, this.validatorUtils);

    // Create test project directory
    // ../
    // ../lib/some-snapshot.jar
    // ../app-meta/startup-dependencies.json
    projectFolder = TEMP_DIR.newFolder("testproj");
    File libFolder = new File(projectFolder, "lib");
    libFolder.mkdirs();
    File appMetaFolder = new File(projectFolder, "app-meta");
    libFolder.mkdirs();
    FileUtils.writeStringToFile(new File(libFolder, ThinArchiveTestSampleData.getDepA().getFile()),
        ThinArchiveTestSampleData.getDepAContent());
    FileUtils.writeStringToFile(new File(appMetaFolder, "startup-dependencies.json"),
        ThinArchiveTestSampleData.getRawJSON());
  }

  @Test
  public void freshValidProject() {
    when(this.storage.getDependency()).thenReturn(this.VERSION);

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    this.archiveUnthinner.validateProjectAndPersistDependencies(this.project, this.projectFolder,
        startupDependenciesFile);


  }
}
