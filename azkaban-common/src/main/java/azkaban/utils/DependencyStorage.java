package azkaban.utils;

import azkaban.db.DatabaseOperator;
import azkaban.spi.StartupDependencyDetails;
import azkaban.spi.Storage;
import java.io.File;
import java.io.IOException;
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
      throws SQLException {
    final ResultSetHandler<Integer> handler = rs -> {
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    };

    return this.dbOperator.query(
        "select count(1) from startup_dependencies where file_sha1 = ? and validation_key = ?",
        handler, d.getSHA1(), validationKey) == 1;
  }

  public void persistDependency(final File file, final StartupDependencyDetails d, final String validationKey)
      throws SQLException, IOException {
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
        throw e;
      }
    }
  }
}
