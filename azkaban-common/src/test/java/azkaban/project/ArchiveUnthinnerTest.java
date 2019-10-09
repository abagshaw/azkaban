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

import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.spi.StartupDependencyDetails;
import azkaban.test.executions.ThinArchiveTestSampleData;
import azkaban.utils.DependencyDownloader;
import azkaban.utils.DependencyStorage;
import azkaban.utils.ThinArchiveUtils;
import azkaban.utils.ValidatorUtils;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ArchiveUnthinnerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  public final String VALIDATOR_KEY = "somerandomhash";

  private final Project project = new Project(107, "Test_Project");
  private File projectFolder;

  private StartupDependencyDetails depA;
  private StartupDependencyDetails depB;
  private File depAInArtifactory;
  private File depBInArtifactory;

  private ArchiveUnthinner archiveUnthinner;
  private ValidatorUtils validatorUtils;
  private DependencyStorage dependencyStorage;
  private DependencyDownloader dependencyDownloader;



  @Before
  public void setUp() throws Exception {
    this.validatorUtils = mock(ValidatorUtils.class);
    this.dependencyStorage = mock(DependencyStorage.class);
    this.dependencyDownloader = mock(DependencyDownloader.class);
    this.archiveUnthinner = new ArchiveUnthinner(this.validatorUtils,
        this.dependencyStorage, this.dependencyDownloader);

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
        ThinArchiveTestSampleData.getRawJSONBothDeps());

    // Setup sample dependencies
    depA = ThinArchiveTestSampleData.getDepA();
    depB = ThinArchiveTestSampleData.getDepB();
    depAInArtifactory = TEMP_DIR.newFile(depA.getFile());
    depBInArtifactory = TEMP_DIR.newFile(depB.getFile());
    FileUtils.writeStringToFile(depAInArtifactory, ThinArchiveTestSampleData.getDepAContent());
    FileUtils.writeStringToFile(depBInArtifactory, ThinArchiveTestSampleData.getDepBContent());

    // When downloadDependency() is called, write the content to the file as if it was downloaded
    doAnswer((Answer) invocation -> {
      File destFile = (File) invocation.getArguments()[0];
      StartupDependencyDetails requestDependency = (StartupDependencyDetails) invocation.getArguments()[1];

      String contentToWrite = requestDependency.equals(depA) ?
          ThinArchiveTestSampleData.getDepAContent() :
          ThinArchiveTestSampleData.getDepBContent();

      FileUtils.writeStringToFile(destFile, contentToWrite);
      return null;
    }).when(this.dependencyDownloader)
        .downloadDependency(any(File.class), any(StartupDependencyDetails.class));

    // When the unthinner attempts to get a validatorKey for the project, return our sample one.
    when(this.validatorUtils.getCacheKey(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(VALIDATOR_KEY);
  }

  @Test
  public void testSimpleFreshProject() throws Exception {
    // Indicate that the dependencies are not validated, forcing them to be downloaded from artifactory
    when(this.dependencyStorage.dependencyExistsAndIsValidated(depA, VALIDATOR_KEY)).thenReturn(false);
    when(this.dependencyStorage.dependencyExistsAndIsValidated(depB, VALIDATOR_KEY)).thenReturn(false);

    // When the unthinner attempts to validate the project, return an empty map (indicating that the
    // validator found no errors and made no changes to the project)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport is as expected (empty)
    assertEquals(result, new HashMap<>());

    // Verify that dependencies were persisted to storage
    verify(this.dependencyStorage, Mockito.times(1))
        .persistDependency(any(File.class), eq(depA), eq(VALIDATOR_KEY));
    verify(this.dependencyStorage, Mockito.times(1))
        .persistDependency(any(File.class), eq(depB), eq(VALIDATOR_KEY));

    // Verify that dependencies were removed from project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file is NOT modified
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestSampleData.getRawJSONBothDeps(), finalJSON, false);
  }

  @Test
  public void testFileAlreadyInStorage() throws Exception {
    // Indicate that the depA is validated, but depB is not (forcing depB to be downloaded)
    when(this.dependencyStorage.dependencyExistsAndIsValidated(depA, VALIDATOR_KEY)).thenReturn(true);
    when(this.dependencyStorage.dependencyExistsAndIsValidated(depB, VALIDATOR_KEY)).thenReturn(false);

    // When the unthinner attempts to validate the project, return an empty map (indicating that the
    // validator found no errors and made no changes to the project)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport is as expected (empty)
    assertEquals(result, new HashMap<>());

    // Verify that ONLY depB was persisted to storage, but NOT depA
    verify(this.dependencyStorage, Mockito.never())
        .persistDependency(any(File.class), eq(depA), eq(VALIDATOR_KEY));
    verify(this.dependencyStorage, Mockito.times(1))
        .persistDependency(any(File.class), eq(depB), eq(VALIDATOR_KEY));

    // Verify that no dependencies were added to project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file is NOT modified
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestSampleData.getRawJSONBothDeps(), finalJSON, false);
  }

  @Test
  public void testValidatorDeleteFile() throws Exception {
    // Indicate that the dependencies are not in storage, forcing them to be downloaded from artifactory
    when(this.dependencyStorage.dependencyExistsAndIsValidated(depA, VALIDATOR_KEY)).thenReturn(false);
    when(this.dependencyStorage.dependencyExistsAndIsValidated(depB, VALIDATOR_KEY)).thenReturn(false);

    // When the unthinner attempts to validate the project, return a report indicating that the depA jar
    // was removed.
    File depAInProject = new File(projectFolder, depA.getDestination() + File.separator + depA.getFile());
    Set<File> removedFiles = new HashSet();
    removedFiles.add(depAInProject);

    doAnswer((Answer<Map>) invocation -> {
      depAInProject.delete();

      ValidationReport sampleReport = new ValidationReport();
      sampleReport.addRemovedFiles(removedFiles);

      Map<String, ValidationReport> allReports = new HashMap<>();
      allReports.put("sample", sampleReport);

      return allReports;
    }).when(this.validatorUtils).validateProject(eq(this.project), eq(this.projectFolder), any());

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport indicates the correct removed and modified files.
    assertEquals(removedFiles, result.get("sample").getRemovedFiles());
    assertEquals(0, result.get("sample").getModifiedFiles().size());

    // Verify that ONLY depB was persisted to storage, but NOT depA
    verify(this.dependencyStorage, Mockito.never())
        .persistDependency(any(File.class), eq(depA), eq(VALIDATOR_KEY));
    verify(this.dependencyStorage, Mockito.times(1))
        .persistDependency(any(File.class), eq(depB), eq(VALIDATOR_KEY));

    // Verify that no dependencies were added to project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file now contains ONLY depB
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestSampleData.getRawJSONDepB(), finalJSON, false);
  }

  @Test
  public void testValidatorModifyFile() throws Exception {
    // Indicate that the dependencies are not in storage, forcing them to be downloaded from artifactory
    when(this.dependencyStorage.dependencyExistsAndIsValidated(depA, VALIDATOR_KEY)).thenReturn(false);
    when(this.dependencyStorage.dependencyExistsAndIsValidated(depB, VALIDATOR_KEY)).thenReturn(false);

    // When the unthinner attempts to validate the project, return a report indicating that the depA jar
    // was modified.
    File depAInProject = new File(projectFolder, depA.getDestination() + File.separator + depA.getFile());
    Set<File> modifiedFiles = new HashSet();
    modifiedFiles.add(depAInProject);

    doAnswer((Answer<Map>) invocation -> {
      ValidationReport sampleReport = new ValidationReport();
      sampleReport.addModifiedFiles(modifiedFiles);

      Map<String, ValidationReport> allReports = new HashMap<>();
      allReports.put("sample", sampleReport);

      return allReports;
    }).when(this.validatorUtils).validateProject(eq(this.project), eq(this.projectFolder), any());

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport indicates the correct removed and modified files.
    assertEquals(modifiedFiles, result.get("sample").getModifiedFiles());
    assertEquals(0, result.get("sample").getRemovedFiles().size());

    // Verify that ONLY depB was persisted to storage, but NOT depA
    verify(this.dependencyStorage, Mockito.never())
        .persistDependency(any(File.class), eq(depA), eq(VALIDATOR_KEY));
    verify(this.dependencyStorage, Mockito.times(1))
        .persistDependency(any(File.class), eq(depB), eq(VALIDATOR_KEY));

    // Verify that depA remains in the projectFolder (total of two jars)
    assertEquals(2, new File(projectFolder, depA.getDestination()).listFiles().length);
    assertTrue(depAInProject.exists());

    // Verify that the startup-dependencies.json file now contains ONLY depB
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestSampleData.getRawJSONDepB(), finalJSON, false);
  }

  @Test
  public void testReportWithError() throws Exception {
    // Indicate that the dependencies are not in storage, forcing them to be downloaded from artifactory
    when(this.dependencyStorage.dependencyExistsAndIsValidated(depA, VALIDATOR_KEY)).thenReturn(false);
    when(this.dependencyStorage.dependencyExistsAndIsValidated(depB, VALIDATOR_KEY)).thenReturn(false);

    // When the unthinner attempts to validate the project, return a report with ValidationStatus.ERROR
    doAnswer((Answer<Map>) invocation -> {
      String someErrorMsg = "Big problem!";
      Set<String> errMsgs = new HashSet();
      errMsgs.add(someErrorMsg);

      ValidationReport sampleReport = new ValidationReport();
      sampleReport.addErrorMsgs(errMsgs);

      Map<String, ValidationReport> allReports = new HashMap<>();
      allReports.put("sample", sampleReport);

      return allReports;
    }).when(this.validatorUtils).validateProject(eq(this.project), eq(this.projectFolder), any());

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport has an ERROR status.
    assertEquals(ValidationStatus.ERROR, result.get("sample").getStatus());
  }
}
