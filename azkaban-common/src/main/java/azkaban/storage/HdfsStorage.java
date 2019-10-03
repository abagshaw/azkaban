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

import static azkaban.utils.StorageUtils.createTargetDependencyFilename;
import static azkaban.utils.StorageUtils.createTargetProjectFilename;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.ProjectStorageMetadata;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


@Singleton
public class HdfsStorage implements Storage {
  private static final Logger log = Logger.getLogger(HdfsStorage.class);
  private static final String HDFS_SCHEME = "hdfs";
  public static final String DEPENDENCY_FOLDER = "startup_dependencies";

  private final HdfsAuth hdfsAuth;
  private final URI rootUri;
  private final FileSystem hdfs;

  private final Path dependencyPath;

  @Inject
  public HdfsStorage(final HdfsAuth hdfsAuth, final FileSystem hdfs,
      final AzkabanCommonModuleConfig config) throws IOException {
    this.hdfsAuth = requireNonNull(hdfsAuth);
    this.hdfs = requireNonNull(hdfs);

    this.rootUri = config.getHdfsRootUri();
    requireNonNull(this.rootUri.getAuthority(), "URI must have host:port mentioned.");
    checkArgument(HDFS_SCHEME.equals(this.rootUri.getScheme()));

    this.dependencyPath = new Path(this.rootUri.getPath(), DEPENDENCY_FOLDER);
    if (this.hdfs.mkdirs(this.dependencyPath)) {
      log.info("Created dir for jar dependencies: " + this.dependencyPath);
    }
  }

  @Override
  public InputStream getProject(final String key) throws IOException {
    this.hdfsAuth.authorize();
    return this.hdfs.open(fullPath(key));
  }

  @Override
  public String putProject(final ProjectStorageMetadata metadata, final File localFile) {
    this.hdfsAuth.authorize();
    final Path projectsPath = new Path(this.rootUri.getPath(),
        String.valueOf(metadata.getProjectId()));
    try {
      if (this.hdfs.mkdirs(projectsPath)) {
        log.info("Created project dir: " + projectsPath);
      }
      final Path targetPath = new Path(projectsPath,
          createTargetProjectFilename(metadata.getProjectId(), metadata.getHash()));
      if (this.hdfs.exists(targetPath)) {
        log.info(
            String.format("Duplicate Found: meta: %s path: %s", metadata, targetPath));
        return getRelativePath(targetPath);
      }

      // Copy file to HDFS
      log.info(String.format("Creating project artifact: meta: %s path: %s", metadata, targetPath));
      this.hdfs.copyFromLocalFile(new Path(localFile.getAbsolutePath()), targetPath);
      return getRelativePath(targetPath);
    } catch (final IOException e) {
      log.error("error in put(): Metadata: " + metadata);
      throw new StorageException(e);
    }
  }

  @Override
  public void putDependency(File localFile, String name, String sha1) {
    this.hdfsAuth.authorize();
    try {
      // Copy file to HDFS
      final Path targetPath = getDependencyPath(name, sha1);
      log.info(String.format("Uploading dependency to HDFS: %s -> %s", name, targetPath));
      this.hdfs.copyFromLocalFile(new Path(localFile.getAbsolutePath()), targetPath);
    } catch (final FileAlreadyExistsException e) {
      // Either the file already exists, or another webserver process is uploading it
      // Either way, we can assume that the dependency will be present on HDFS and we don't
      // need to worry about persisting it.
      log.info("Upload stopped. Dependency already exists in HDFS: " + name);
    } catch (final IOException e) {
      log.error("Error uploading dependency to HDFS: " + name);
      throw new StorageException(e);
    }
  }

  @Override
  public InputStream getDependency(String name, String sha1) throws IOException {
    this.hdfsAuth.authorize();
    return this.hdfs.open(getDependencyPath(name, sha1));
  }

  @Override
  public boolean existsDependency(String name, String sha1) throws IOException {
    this.hdfsAuth.authorize();
    return this.hdfs.exists(getDependencyPath(name, sha1));
  }

  private Path getDependencyPath(String name, String sha1) {
    return new Path(this.dependencyPath, createTargetDependencyFilename(name, sha1));
  }

  private String getRelativePath(final Path targetPath) {
    return URI.create(this.rootUri.getPath()).relativize(targetPath.toUri()).getPath();
  }

  @Override
  public boolean delete(final String key) {
    this.hdfsAuth.authorize();
    final Path path = fullPath(key);
    try {
      return this.hdfs.delete(path, false);
    } catch (final IOException e) {
      log.error("HDFS delete failed on " + path, e);
      return false;
    }
  }

  private Path fullPath(final String key) {
    return new Path(this.rootUri.toString(), key);
  }
}
