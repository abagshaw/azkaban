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

import azkaban.db.DatabaseOperator;
import azkaban.project.ProjectFileHandler;
import azkaban.project.ProjectLoader;
import azkaban.spi.AzkabanException;
import azkaban.spi.Storage;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.utils.Props;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import javax.inject.Singleton;
import java.io.File;
import java.io.InputStream;
import javax.inject.Inject;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


/**
 * DatabaseStorage
 *
 * This class helps in storing projects in the DB itself. This is intended to be the default since
 * it is the current behavior of Azkaban.
 */
@Singleton
public class DatabaseStorage implements Storage {

  private static final Logger logger = Logger.getLogger(DatabaseStorage.class);

  private final ProjectLoader projectLoader;
  private final DatabaseOperator dbOperator;
  private final File tempDir;

  final String INSERT_DEPENDENCY_FILE =
      "INSERT INTO dependency_files (name, hash, file) values (?,?,?)";

  final String SELECT_DEPENDENCY_FILE =
      "SELECT * FROM dependency_files WHERE name=? AND hash=?";

  @Inject
  public DatabaseStorage(final Props props, final ProjectLoader projectLoader, final DatabaseOperator databaseOperator) {
    this.projectLoader = projectLoader;
    this.dbOperator = databaseOperator;

    this.tempDir = new File(props.getString("project.temp.dir", "temp"));
  }

  @Override
  public InputStream getProject(final String key) {
    throw new UnsupportedOperationException(
        "Not implemented yet. Use get(projectId, version) instead");
  }

  public ProjectFileHandler getProject(final int projectId, final int version) {
    return this.projectLoader.getUploadedFile(projectId, version);
  }

  @Override
  public String putProject(final ProjectStorageMetadata metadata, final File localFile) {
    this.projectLoader.uploadProjectFile(
        metadata.getProjectId(),
        metadata.getVersion(),
        localFile, metadata.getUploader());

    return null;
  }

  @Override
  public void putDependency(final File localFile, String name, String hash) throws AzkabanException {
    logger.info(String
        .format("Uploading dependency: %s [%d bytes]", localFile.getName(),
            localFile.length()));

    byte[] fileBytes = null;
    try {
      fileBytes = FileUtils.readFileToByteArray(localFile);
    } catch (IOException e) {
      logger.error("Could not read local dependency file.", e);
      throw new AzkabanException("Could not read local dependency file.", e);
    }

    try {
      this.dbOperator.update(INSERT_DEPENDENCY_FILE, name, hash, fileBytes);
    } catch (final SQLException e) {
      logger.error("Uploading dependency failed.", e);
      throw new AzkabanException("Uploading dependency failed.", e);
    }

    logger.info(String.format("Finished uploading dependency: %s [%d bytes]", localFile.getName(),
        localFile.length()));
  }

  @Override
  public InputStream getDependency(String name, String hash) {
    logger.info(String
        .format("Fetching dependency: %s", name));

    ResultSetHandler<byte[]> handler = rs -> {
      if (!rs.next()) {
        return null;
      }
      return rs.getBytes("file");
    };

    byte[] fileBytes;
    try {
      fileBytes = this.dbOperator.query(SELECT_DEPENDENCY_FILE, handler, name, hash);
    } catch (final SQLException e) {
      logger.error("Fetching dependency failed.", e);
      throw new AzkabanException("Fetching dependency failed.", e);
    }

    InputStream stream = new ByteArrayInputStream(fileBytes);
    logger.info(String.format("Finished fetching dependency: %s [%d bytes]", name,
        fileBytes.length));

    return stream;
  }

  @Override
  public boolean delete(final String key) {
    throw new UnsupportedOperationException("Delete is not supported");
  }
}
