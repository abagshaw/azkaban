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
import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.FileStatus;
import azkaban.spi.FileValidationStatus;
import azkaban.spi.Storage;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.DependencyDownloader;
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
import org.mockito.stubbing.Answer;
import org.skyscreamer.jsonassert.JSONAssert;

import static azkaban.test.executions.ThinArchiveTestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ArchiveUnthinnerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  public final String VALIDATION_KEY = "somerandomhash";

  private final Project project = new Project(107, "Test_Project");
  private File projectFolder;

  private Dependency depA;
  private Dependency depB;
  private File depAInArtifactory;
  private File depBInArtifactory;

  private ArchiveUnthinner archiveUnthinner;
  private ValidatorUtils validatorUtils;
  private JdbcDependencyManager jdbcDependencyManager;
  private DependencyDownloader dependencyDownloader;
  private Storage storage;

  @Before
  public void setUp() throws Exception {
    this.validatorUtils = mock(ValidatorUtils.class);
    this.jdbcDependencyManager = mock(JdbcDependencyManager.class);
    this.dependencyDownloader = mock(DependencyDownloader.class);
    this.storage = mock(Storage.class);
    this.archiveUnthinner = new ArchiveUnthinner(this.validatorUtils,
        this.jdbcDependencyManager, this.dependencyDownloader, this.storage);

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
        ThinArchiveTestUtils.getRawJSONDepsAB());

    // Setup sample dependencies
    depA = ThinArchiveTestUtils.getDepA();
    depB = ThinArchiveTestUtils.getDepB();
    depAInArtifactory = TEMP_DIR.newFile(depA.getFileName());
    depBInArtifactory = TEMP_DIR.newFile(depB.getFileName());
    FileUtils.writeStringToFile(depAInArtifactory, ThinArchiveTestUtils.getDepAContent());
    FileUtils.writeStringToFile(depBInArtifactory, ThinArchiveTestUtils.getDepBContent());

    // When downloadDependency() is called, write the content to the file as if it was downloaded
    doAnswer((Answer) invocation -> {
      DependencyFile destDep = (DependencyFile) invocation.getArguments()[0];

      String contentToWrite = destDep.equals(depA) ?
          ThinArchiveTestUtils.getDepAContent() :
          ThinArchiveTestUtils.getDepBContent();

      FileUtils.writeStringToFile(destDep.getFile(), contentToWrite);
      return null;
    }).when(this.dependencyDownloader)
        .downloadDependency(any(DependencyFile.class));

    // When the unthinner attempts to get a validationKey for the project, return our sample one.
    when(this.validatorUtils.getCacheKey(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(VALIDATION_KEY);
  }

  @Test
  public void testAllNew() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return an empty map (indicating that the
    // validator found no errors and made no changes to the project)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    // Indicate both deps persisted successfully
    when(this.storage.putDependency(depEq(depA))).thenReturn(FileStatus.CLOSED);
    when(this.storage.putDependency(depEq(depB))).thenReturn(FileStatus.CLOSED);

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport is as expected (empty)
    assertEquals(result, new HashMap<>());

    // Verify that both dependencies were persisted to storage
    verify(this.storage).putDependency(depEq(depA));
    verify(this.storage).putDependency(depEq(depB));

    // Verify that both dependencies were added to DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depA, FileValidationStatus.VALID);
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that dependencies were removed from project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file is NOT modified
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepsAB(), finalJSON, false);
  }

  @Test
  public void testCachedValid() throws Exception {
    // Indicate that the depA is cached VALID, but depB is NEW (forcing depB to be downloaded)
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.VALID);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return an empty map (indicating that the
    // validator found no errors and made no changes to the project)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    // Indicate depB persisted successfully
    when(this.storage.putDependency(depEq(depB))).thenReturn(FileStatus.CLOSED);

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport is as expected (empty)
    assertEquals(result, new HashMap<>());

    // Verify that ONLY depB was persisted to storage
    verify(this.storage, never()).putDependency(depEq(depA));
    verify(this.storage).putDependency(depEq(depB));

    // Verify that ONLY depB was added to DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that no dependencies were added to project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file is NOT modified
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepsAB(), finalJSON, false);
  }

  @Test
  public void testCachedRemoved() throws Exception {
    // Indicate that depA is cached REMOVED, but depB is NEW (forcing depB to be downloaded)
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.REMOVED);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return an empty map (indicating that the
    // validator found no errors and made no changes to the project)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    // Indicate depB persisted successfully
    when(this.storage.putDependency(depEq(depB))).thenReturn(FileStatus.CLOSED);

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport is as expected (empty)
    assertEquals(result, new HashMap<>());

    // Verify that ONLY depB was persisted to storage
    verify(this.storage, never()).putDependency(depEq(depA));
    verify(this.storage).putDependency(depEq(depB));

    // Verify that ONLY depB was added to DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that no dependencies were added to project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file now contains ONLY depB
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepB(), finalJSON, false);
  }

  @Test
  public void testValidatorDeleteFile() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return a report indicating that the depA jar
    // was removed.
    File depAInProject = new File(projectFolder, depA.getDestination() + File.separator + depA.getFileName());
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

    // Indicate depB persisted successfully
    when(this.storage.putDependency(depEq(depB))).thenReturn(FileStatus.CLOSED);

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport indicates the correct removed and modified files.
    assertEquals(removedFiles, result.get("sample").getRemovedFiles());
    assertEquals(0, result.get("sample").getModifiedFiles().size());

    // Verify that ONLY depB was persisted to storage
    verify(this.storage, never()).putDependency(depEq(depA));
    verify(this.storage).putDependency(depEq(depB));

    // Verify that depA was added to DB as REMOVED and depB was added as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depA, FileValidationStatus.REMOVED);
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that no dependencies were added to project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file now contains ONLY depB
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepB(), finalJSON, false);
  }

  @Test
  public void testValidatorModifyFile() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return a report indicating that the depA jar
    // was modified.
    File depAInProject = new File(projectFolder, depA.getDestination() + File.separator + depA.getFileName());
    Set<File> modifiedFiles = new HashSet();
    modifiedFiles.add(depAInProject);

    doAnswer((Answer<Map>) invocation -> {
      ValidationReport sampleReport = new ValidationReport();
      sampleReport.addModifiedFiles(modifiedFiles);

      Map<String, ValidationReport> allReports = new HashMap<>();
      allReports.put("sample", sampleReport);

      return allReports;
    }).when(this.validatorUtils).validateProject(eq(this.project), eq(this.projectFolder), any());

    // Indicate depB persisted successfully
    when(this.storage.putDependency(depEq(depB))).thenReturn(FileStatus.CLOSED);

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport indicates the correct removed and modified files.
    assertEquals(modifiedFiles, result.get("sample").getModifiedFiles());
    assertEquals(0, result.get("sample").getRemovedFiles().size());

    // Verify that ONLY depB was persisted to storage
    verify(this.storage, never()).putDependency(depEq(depA));
    verify(this.storage).putDependency(depEq(depB));

    // Verify that ONLY depB was added to DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that depA remains in the projectFolder (total of two jars)
    assertEquals(2, new File(projectFolder, depA.getDestination()).listFiles().length);
    assertTrue(depAInProject.exists());

    // Verify that the startup-dependencies.json file now contains ONLY depB
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepB(), finalJSON, false);
  }

  @Test
  public void testUnsuccessfulPersist() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return an empty map (indicating that the
    // validator found no errors and made no changes to the project)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    // Indicate ONLY depA persisted successfully, depB may still be OPEN
    when(this.storage.putDependency(depEq(depA))).thenReturn(FileStatus.CLOSED);
    when(this.storage.putDependency(depEq(depB))).thenReturn(FileStatus.OPEN);

    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(this.projectFolder);
    Map<String, ValidationReport> result = this.archiveUnthinner
        .validateProjectAndPersistDependencies(this.project, this.projectFolder, startupDependenciesFile,
            null);

    // Verify that ValidationReport is as expected (empty)
    assertEquals(result, new HashMap<>());

    // Verify that both dependencies were ATTEMPTED to be persisted to storage
    verify(this.storage).putDependency(depEq(depA));
    verify(this.storage).putDependency(depEq(depB));

    // Verify that ONLY depA was added to the DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depA, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that dependencies were removed from project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file is NOT modified
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepsAB(), finalJSON, false);
  }

  @Test
  public void testReportWithError() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

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

    // Verify that no dependencies were persisted
    verify(this.storage, never()).putDependency(any());

    // Verify that nothing was updated in DB
    verify(this.jdbcDependencyManager, never()).updateValidationStatuses(any(), any());
  }
}
