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

package azkaban.utils;

import azkaban.spi.StartupDependencyDetails;
import azkaban.spi.Storage;
import azkaban.test.executions.ThinArchiveTestSampleData;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.junit.Assert.*;

public class ThinArchiveUtilsTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  @Test
  public void testGetStartupDependenciesFile() throws Exception {
    File someFolder = TEMP_DIR.newFolder("someproject");
    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(someFolder);

    assertEquals(someFolder.getAbsolutePath() + "/app-meta/startup-dependencies.json", startupDependenciesFile.getAbsolutePath());
  }

  @Test
  public void testGetDependencyFile() throws Exception {
    File someFolder = TEMP_DIR.newFolder("someproject");

    File dependencyFile = ThinArchiveUtils.getDependencyFile(someFolder, ThinArchiveTestSampleData.getDepA());
    File expectedDependencyFile = new File(someFolder, ThinArchiveTestSampleData.getDepA().getDestination()
        + File.separator
        + ThinArchiveTestSampleData.getDepA().getFile());

    assertEquals(expectedDependencyFile, dependencyFile);
  }

  @Test
  public void testWriteStartupDependencies() throws Exception {
    File outFile = TEMP_DIR.newFile("startup-dependencies.json");
    ThinArchiveUtils.writeStartupDependencies(outFile, ThinArchiveTestSampleData.getDepList());

    String writtenJSON = FileUtils.readFileToString(outFile);
    JSONAssert.assertEquals(ThinArchiveTestSampleData.getRawJSONBothDeps(), writtenJSON, false);
  }

  @Test
  public void testReadStartupDependencies() throws Exception {
    File inFile = TEMP_DIR.newFile("startup-dependencies.json");
    FileUtils.writeStringToFile(inFile, ThinArchiveTestSampleData.getRawJSONBothDeps());

    List<StartupDependencyDetails> parsedDependencies = ThinArchiveUtils.parseStartupDependencies(inFile);
    assertEquals(ThinArchiveTestSampleData.getDepList(), parsedDependencies);
  }

  @Test
  public void testConvertIvyCoordinateToPath() throws Exception {
    assertEquals(ThinArchiveTestSampleData.getDepAPath(),
        ThinArchiveUtils.convertIvyCoordinateToPath(ThinArchiveTestSampleData.getDepA()));
  }

  @Test
  public void testReplaceLocalPathsWithStoragePathsTHIN() throws Exception {
    // This is a test on a project from a THIN archive (and we expect the path for the one dependency listed
    // in startup-dependencies.json, depA) to be replaced with the HDFS path, but the other jar should still have
    // its normal local filepath

    StartupDependencyDetails depA = ThinArchiveTestSampleData.getDepA();
    File projectDir = TEMP_DIR.newFolder("sample_proj");

    String HDFS_DEP_PREFIX = "hdfs://some/cool/place/";

    Props props = new Props();
    props.put(Storage.DEPENDENCY_STORAGE_PATH_PREFIX_PROP, HDFS_DEP_PREFIX);

    // Put depA in the correct location
    File depAFile = new File(projectDir, depA.getDestination() + File.separator + depA.getFile());
    FileUtils.writeStringToFile(depAFile, ThinArchiveTestSampleData.getDepAContent());

    // Put some other random jar in the same folder as depA
    File otherRandomJar = new File(projectDir, depA.getDestination() + File.separator + "blahblah-1.0.0.jar");
    FileUtils.writeStringToFile(otherRandomJar, "somerandomcontent");

    // Write the startup-dependencies.json file
    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(projectDir);
    FileUtils.writeStringToFile(startupDependenciesFile, ThinArchiveTestSampleData.getRawJSONDepA());

    List<String> jarPaths = new ArrayList<>();
    jarPaths.add(depAFile.getCanonicalPath());
    jarPaths.add(otherRandomJar.getCanonicalPath());

    List<String> resultingJarPaths = ThinArchiveUtils.replaceLocalPathsWithStoragePaths(projectDir, jarPaths, props);


    List<String> expectedResultingJarPaths = new ArrayList<>();
    expectedResultingJarPaths.add(HDFS_DEP_PREFIX + ThinArchiveUtils.convertIvyCoordinateToPath(depA));
    expectedResultingJarPaths.add(jarPaths.get(1));

    assertEquals(expectedResultingJarPaths, resultingJarPaths);
  }

  @Test
  public void testReplaceLocalPathsWithStoragePathsFAT() throws Exception {
    // This is a test on a project from a FAT archive (essentially because there will be no startup-depencencies.json
    // file in this project, we expect none of the paths to be replaced (the input paths should be returned without
    // any modification)

    StartupDependencyDetails depA = ThinArchiveTestSampleData.getDepA();
    File projectDir = TEMP_DIR.newFolder("sample_proj");

    String HDFS_DEP_PREFIX = "hdfs://some/cool/place/";

    Props props = new Props();
    props.put(Storage.DEPENDENCY_STORAGE_PATH_PREFIX_PROP, HDFS_DEP_PREFIX);

    // Put depA in the correct location
    File depAFile = new File(projectDir, depA.getDestination() + File.separator + depA.getFile());
    FileUtils.writeStringToFile(depAFile, ThinArchiveTestSampleData.getDepAContent());

    List<String> jarPaths = new ArrayList<>();
    jarPaths.add(depAFile.getCanonicalPath());

    List<String> expectedResultingJarPaths = new ArrayList<>();
    expectedResultingJarPaths.add(jarPaths.get(0));

    List<String> resultingJarPaths = ThinArchiveUtils.replaceLocalPathsWithStoragePaths(projectDir, jarPaths, props);

    assertEquals(expectedResultingJarPaths, resultingJarPaths);
  }

  @Test
  public void testReplaceLocalPathsWithStoragePathsMalformedTHIN() throws Exception {
    // This is a test on a project from a THIN archive but with a malformed startup-dependencies.json file
    // we expect the original local file paths to be returned, with no modifications and no exceptions thrown.

    StartupDependencyDetails depA = ThinArchiveTestSampleData.getDepA();
    File projectDir = TEMP_DIR.newFolder("sample_proj");

    String HDFS_DEP_PREFIX = "hdfs://some/cool/place/";

    Props props = new Props();
    props.put(Storage.DEPENDENCY_STORAGE_PATH_PREFIX_PROP, HDFS_DEP_PREFIX);

    // Put depA in the correct location
    File depAFile = new File(projectDir, depA.getDestination() + File.separator + depA.getFile());
    FileUtils.writeStringToFile(depAFile, ThinArchiveTestSampleData.getDepAContent());

    // Write the startup-dependencies.json file
    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(projectDir);
    FileUtils.writeStringToFile(startupDependenciesFile, "MALFORMED JSON BLAHBLAH");

    List<String> jarPaths = new ArrayList<>();
    jarPaths.add(depAFile.getCanonicalPath());

    List<String> expectedResultingJarPaths = new ArrayList<>();
    expectedResultingJarPaths.add(jarPaths.get(0));

    List<String> resultingJarPaths = ThinArchiveUtils.replaceLocalPathsWithStoragePaths(projectDir, jarPaths, props);

    assertEquals(expectedResultingJarPaths, resultingJarPaths);
  }

  @Test
  public void testValidateDependencyHashValid() throws Exception {
    File depFile = TEMP_DIR.newFile("dep.jar");
    FileUtils.writeStringToFile(depFile, ThinArchiveTestSampleData.getDepAContent());

    // This should complete without an exception
    ThinArchiveUtils.validateDependencyHash(depFile, ThinArchiveTestSampleData.getDepA());
  }

  @Test(expected = HashNotMatchException.class)
  public void testValidateDependencyHashInvalid() throws Exception {
    File depFile = TEMP_DIR.newFile("dep.jar");
    String depFileHash = HashUtils.bytesHashToString(HashUtils.SHA1.getHash(depFile));

    StartupDependencyDetails details = new StartupDependencyDetails(
        "dep.jar",
        "lib",
        "jar",
        "com.linkedin.test:blahblah:1.0.1",
        "73f018101ec807672cd3b06d5d7a0fc48f54428f"); // This is not the hash of depFile

    // This should throw an exception because the hashes don't match
    ThinArchiveUtils.validateDependencyHash(depFile, details);
  }
}
