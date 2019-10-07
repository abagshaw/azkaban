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
import azkaban.test.executions.ThinArchiveTestSampleData;
import java.io.File;
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
  public void testWriteStartupDependencies() throws Exception {
    File outFile = TEMP_DIR.newFile("startup-dependencies.json");
    ThinArchiveUtils.writeStartupDependencies(outFile, ThinArchiveTestSampleData.getDepList());

    String writtenJSON = FileUtils.readFileToString(outFile);
    JSONAssert.assertEquals(ThinArchiveTestSampleData.getRawJSON(), writtenJSON, false);
  }

  @Test
  public void testReadStartupDependencies() throws Exception {
    File inFile = TEMP_DIR.newFile("startup-dependencies.json");
    FileUtils.writeStringToFile(inFile, ThinArchiveTestSampleData.getRawJSON());

    List<StartupDependencyDetails> parsedDependencies = ThinArchiveUtils.parseStartupDependencies(inFile);
    assertEquals(ThinArchiveTestSampleData.getDepList(), parsedDependencies);
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
