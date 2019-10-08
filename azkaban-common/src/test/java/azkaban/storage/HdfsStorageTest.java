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

package azkaban.storage;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.test.executions.ThinArchiveTestSampleData;
import azkaban.utils.HashUtils;
import java.io.File;
import java.net.URI;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class HdfsStorageTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private HdfsAuth hdfsAuth;
  private HdfsStorage hdfsStorage;
  private FileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    this.hdfs = mock(FileSystem.class);
    this.hdfsAuth = mock(HdfsAuth.class);
    final AzkabanCommonModuleConfig config = mock(AzkabanCommonModuleConfig.class);
    when(config.getHdfsRootUri()).thenReturn(URI.create("hdfs://localhost:9000/path/to/foo"));

    this.hdfsStorage = new HdfsStorage(this.hdfsAuth, this.hdfs, config);
  }

  @Test
  public void testGetProject() throws Exception {
    this.hdfsStorage.getProject("1/1-hash.zip");
    verify(this.hdfs).open(new Path("hdfs://localhost:9000/path/to/foo/1/1-hash.zip"));
  }

  @Test
  public void testPutProject() throws Exception {
    final File file = new File(
        getClass().getClassLoader().getResource("sample_flow_01.zip").getFile());
    final String hash = new String(Hex.encodeHex(HashUtils.MD5.getHash(file)));

    when(this.hdfs.exists(any(Path.class))).thenReturn(false);

    final ProjectStorageMetadata metadata = new ProjectStorageMetadata(1, 2, "uploader", HashUtils.MD5.getHash(file));
    final String key = this.hdfsStorage.putProject(metadata, file);

    final String expectedName = String.format("1/1-%s.zip", hash);
    Assert.assertEquals(expectedName, key);

    final String expectedPath = "/path/to/foo/" + expectedName;
    verify(this.hdfs).copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(expectedPath));
  }

  @Test
  public void testGetDependency() throws Exception {
    this.hdfsStorage.getDependency(ThinArchiveTestSampleData.getDepA());
    verify(this.hdfs).open(new Path("/path/to/foo/"
            + HdfsStorage.DEPENDENCY_FOLDER + "/"
            + ThinArchiveTestSampleData.getDepAPath()));
  }

  @Test
  public void testExistsDependency() throws Exception {
    this.hdfsStorage.existsDependency(ThinArchiveTestSampleData.getDepA());
    verify(this.hdfs).exists(new Path("/path/to/foo/"
        + HdfsStorage.DEPENDENCY_FOLDER + "/"
        + ThinArchiveTestSampleData.getDepAPath()));
  }

  @Test
  public void testPutDependency() throws Exception {
    final File tmpEmptyJar = TEMP_DIR.newFile(ThinArchiveTestSampleData.getDepA().getFile());
    this.hdfsStorage.putDependency(tmpEmptyJar, ThinArchiveTestSampleData.getDepA());

    final String expectedPath = "/path/to/foo/" +
        HdfsStorage.DEPENDENCY_FOLDER + "/"
        + ThinArchiveTestSampleData.getDepAPath();
    verify(this.hdfs).copyFromLocalFile(new Path(tmpEmptyJar.getAbsolutePath()), new Path(expectedPath));
  }

  @Test
  public void testDelete() throws Exception {
    this.hdfsStorage.delete("1/1-hash.zip");
    verify(this.hdfs).delete(new Path("hdfs://localhost:9000/path/to/foo/1/1-hash.zip"), false);
  }
}
