package azkaban.utils;

import azkaban.db.DatabaseOperator;
import azkaban.executor.ExecutionFlowDao;
import azkaban.spi.StartupDependencyDetails;
import azkaban.spi.Storage;
import java.io.File;
import java.sql.SQLException;
import javax.inject.Inject;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;


public class DependencyStorage {
  private final DatabaseOperator dbOperator;
  private final Storage storage;

  @Inject
  DependencyStorage(final DatabaseOperator dbOperator, final Storage storage) {
    this.storage = storage;
    this.dbOperator = dbOperator;
  }

  public boolean dependencyExistsAndIsValidated(final StartupDependencyDetails d, final String validationKey)
      throws Exception {
    try {
      return this.dbOperator.query(
          "select count(1) from startup_dependencies where sha1 = ? and validation_key = ?",
          new ScalarHandler<>(), d.getSHA1(), validationKey);
    } catch (final SQLException e) {
      throw new Exception("Unable to query dependency validation table.", e);
    }
  }

  public void persistDependency(final StartupDependencyDetails d, final String validationKey, final File file)
      throws Exception {
    if (!this.storage.existsDependency(d)) {
      this.storage.putDependency(file, d);
    }

    try {
      this.dbOperator.update(
          "insert into startup_dependencies values (?, ?)",
          d.getSHA1(), validationKey);
    } catch (final SQLException e) {
      // 1062 is the error code for a duplicate entry in MySQL, so we assume that the entry already exists
      // and can silently swallow this exception if we get error code 1062.
      if (e.getErrorCode() != 1062) {
        throw new Exception(
            String.format("Unable to insert cache key in startup_dependencies. SHA1: %s, validation_key: %s",
                d.getSHA1(), validationKey), e);
      }
    }
  }
}
