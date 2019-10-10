package azkaban.project;

import azkaban.db.DatabaseOperator;
import azkaban.spi.Dependency;
import azkaban.spi.Storage;
import azkaban.spi.FileValidationStatus;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
    Map<String, Dependency> hashToDep = new HashMap<>();
    PreparedStatement stmnt = this.dbOperator.getDataSource().getConnection().prepareStatement(
        "select file_sha1, validation_status from validated_dependencies where validation_key = ? and file_sha1 in ("
            + makeStrWithQuestionMarks(deps.size()) + ")");

    stmnt.setString(1, validationKey);

    // Start at 2 because the first parameter is at index 1, and that is the validator key that we already set.
    int index = 2;
    for (Dependency d : deps) {
      stmnt.setString(index++, d.getSHA1());
      hashToDep.put(d.getSHA1(), d);
    }

    ResultSet rs = stmnt.executeQuery();

    Map<Dependency, FileValidationStatus> depValidationStatuses = new HashMap<>();
    while (rs.next()) {
      Dependency d = hashToDep.remove(rs.getString(1));
      FileValidationStatus v = FileValidationStatus.valueOf(rs.getInt(2));
      depValidationStatuses.put(d, v);
    }

    // All remaining dependencies in the hashToDep map should be marked as being NEW (because they weren't
    // associated with any DB entry
    hashToDep.values().stream().forEach(d -> depValidationStatuses.put(d, FileValidationStatus.NEW));

    return depValidationStatuses;
  }

  public void updateValidationStatuses(final Map<Dependency, FileValidationStatus> depValidationStatuses,
      final String validationKey) throws SQLException {
    // Order of columns: file_sha1, validation_key, validation_status
    Object[][] rowsToInsert = depValidationStatuses
        .keySet()
        .stream()
        .map(d -> new Object[]{d.getSHA1(), validationKey, depValidationStatuses.get(d).getValue()})
        .toArray(Object[][]::new);

    // We use insert IGNORE because a another process may have been processing the same dependency
    // and written the row for a given dependency before we were able to (resulting in a duplicate primary key
    // error when we try to write the row), so this will ignore the error and continue persisting the other
    // dependencies.
    this.dbOperator.batch("insert ignore into validated_dependencies values (?, ?, ?)", rowsToInsert);
  }

  private String makeStrWithQuestionMarks(final int num) {
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < num; i++) {
      builder.append("?,");
    }
    return builder.deleteCharAt(builder.length() - 1).toString();
  }
}
