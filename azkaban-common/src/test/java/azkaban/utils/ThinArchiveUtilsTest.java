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

import azkaban.project.StartupDependencyDetails;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.junit.Assert.*;

public class ThinArchiveUtilsTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  // The SHA1 of an empty file
  public final String SHA1_EMPTY_FILE = "da39a3ee5e6b4b0d3255bfef95601890afd80709";

  public StartupDependencyDetails depA;
  public StartupDependencyDetails depB;
  public List<StartupDependencyDetails> depList;
  public String rawJSON;

  @Before
  public void setup() {
    depA = new StartupDependencyDetails(
        "a.jar",
        "lib",
        "jar",
        "com.linkedin.test:testera:1.0.1",
        "73f018101ec807672cd3b06d5d7a0fc48f54428f");

    depB = new StartupDependencyDetails(
        "b.jar",
        "lib",
        "jar",
        "com.linkedin.test:testerb:1.0.1",
        "c4ea0f854975e24faf5bb404e8d77915e312e8ab");

    depList = Arrays.asList(depA, depB);

    rawJSON =
        "{" +
        "    \"dependencies\": [" +
        "        {" +
        "            \"sha1\": \"73f018101ec807672cd3b06d5d7a0fc48f54428f\"," +
        "            \"file\": \"a.jar\"," +
        "            \"destination\": \"lib\"," +
        "            \"type\": \"jar\"," +
        "            \"ivyCoordinates\": \"com.linkedin.test:testera:1.0.1\"" +
        "        }," +
        "        {" +
        "            \"sha1\": \"c4ea0f854975e24faf5bb404e8d77915e312e8ab\"," +
        "            \"file\": \"b.jar\"," +
        "            \"destination\": \"lib\"," +
        "            \"type\": \"jar\"," +
        "            \"ivyCoordinates\": \"com.linkedin.test:testerb:1.0.1\"" +
        "        }" +
        "    ]" +
        "}";

  }

  @Test
  public void testGetStartupDependenciesFile() throws Exception {
    File someFolder = TEMP_DIR.newFolder("someproject");
    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(someFolder);

    assertEquals(someFolder.getAbsolutePath() + "/app-meta/startup-dependencies.json", startupDependenciesFile.getAbsolutePath());
  }

  @Test
  public void testWriteStartupDependencies() throws Exception {
    File outFile = TEMP_DIR.newFile("startup-dependencies.json");
    ThinArchiveUtils.writeStartupDependencies(outFile, depList);

    String writtenJSON = FileUtils.readFileToString(outFile);
    JSONAssert.assertEquals(rawJSON, writtenJSON, false);
  }

  @Test
  public void testReadStartupDependencies() throws Exception {
    File inFile = TEMP_DIR.newFile("startup-dependencies.json");
    FileUtils.writeStringToFile(inFile, rawJSON);

    List<StartupDependencyDetails> parsedDependencies = ThinArchiveUtils.parseStartupDependencies(inFile);
    assertEquals(depList, parsedDependencies);
  }

  @Test
  public void testGetArtifactoryUrlForDependency() throws Exception {
    String generatedURL = ThinArchiveUtils.getArtifactoryUrlForDependency(depA);

    assertEquals(
        "http://dev-artifactory.corp.linkedin.com:8081/artifactory/release/com/linkedin/test/testera/1.0.1/a.jar",
        generatedURL);
  }

  @Test
  public void testValidateDependencyHashValid() throws Exception {
    File depFile = TEMP_DIR.newFile("dep.jar");

    StartupDependencyDetails details = new StartupDependencyDetails(
        "dep.jar",
        "lib",
        "jar",
        "com.linkedin.test:blahblah:1.0.1",
        SHA1_EMPTY_FILE);

    // This should complete without an exception
    ThinArchiveUtils.validateDependencyHash(depFile, details);
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
