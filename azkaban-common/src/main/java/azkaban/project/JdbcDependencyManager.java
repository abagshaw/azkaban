package azkaban.project;

import azkaban.db.DatabaseOperator;
import azkaban.spi.Dependency;
import azkaban.spi.Storage;
import azkaban.spi.FileValidationStatus;
import azkaban.utils.HashUtils;
import azkaban.utils.InvalidHashException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides methods for interacting with dependency validation cache in DB. Used during thin archive
 * uploads.
 */
public class JdbcDependencyManager {
  private static final Logger log = LoggerFactory.getLogger(JdbcDependencyManager.class);

  private final DatabaseOperator dbOperator;
  private final Storage storage;

  @Inject
  JdbcDependencyManager(final DatabaseOperator dbOperator, final Storage storage) {
    this.storage = storage;
    this.dbOperator = dbOperator;
  }

  public Map<Dependency, FileValidationStatus> getValidationStatuses(final Set<Dependency> deps,
      final String validationKey) throws SQLException {
    Map<Dependency, FileValidationStatus> depValidationStatuses = new HashMap<>();
    if (deps.size() == 0) {
      // There's nothing for us to do.
      return depValidationStatuses;
    }

    // Map of (sha1 + filename) -> Dependency for resolving the dependencies already cached in the DB
    // after the query completes.
    Map<String, Dependency> hashAndFileNameToDep = new HashMap<>();

    PreparedStatement stmnt = this.dbOperator.getDataSource().getConnection().prepareStatement(
        "select file_sha1, file_name, validation_status from validated_dependencies where validation_key = ? and ("
            + makeStrWithQuestionMarks(deps.size()) + ")");

    stmnt.setString(1, validationKey);

    // Start at 2 because the first parameter is at index 1, and that is the validator key that we already set.
    int index = 2;
    for (Dependency d : deps) {
      stmnt.setString(index++, d.getSHA1());
      stmnt.setString(index++, d.getFileName());
      hashAndFileNameToDep.put(d.getSHA1() + d.getFileName(), d);
    }

    ResultSet rs = stmnt.executeQuery();

    while (rs.next()) {
      // Columns are (starting at index 1): file_sha1, file_name, validation_status
      Dependency d = hashAndFileNameToDep.remove(rs.getString(1) + rs.getString(2));
      FileValidationStatus v = FileValidationStatus.valueOf(rs.getInt(3));
      depValidationStatuses.put(d, v);
    }

    // All remaining dependencies in the hashToDep map should be marked as being NEW (because they weren't
    // associated with any DB entry)
    hashAndFileNameToDep.values().stream().forEach(d -> depValidationStatuses.put(d, FileValidationStatus.NEW));

    return depValidationStatuses;
  }

  public void updateValidationStatuses(final Map<Dependency, FileValidationStatus> depValidationStatuses,
      final String validationKey) throws SQLException {
    if (depValidationStatuses.size() == 0) {
      return;
    }

    // Order of columns: file_sha1, file_name, validation_key, validation_status
    Object[][] rowsToInsert = depValidationStatuses
        .keySet()
        .stream()
        .map(d -> new Object[]{d.getSHA1(), d.getFileName(), validationKey, depValidationStatuses.get(d).getValue()})
        .toArray(Object[][]::new);

    // We use insert IGNORE because a another process may have been processing the same dependency
    // and written the row for a given dependency before we were able to (resulting in a duplicate primary key
    // error when we try to write the row), so this will ignore the error and continue persisting the other
    // dependencies.
    this.dbOperator.batch("insert ignore into validated_dependencies values (?, ?, ?, ?)", rowsToInsert);
  }

  private static String makeStrWithQuestionMarks(final int num) {
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < num; i++) {
      builder.append("(file_sha1 = ? and file_name = ?) or ");
    }
    // Remove trailing " or ";
    return builder.deleteCharAt(builder.length() - 4).toString();
  }
}
