package azkaban.utils;

import azkaban.db.DatabaseOperator;
import azkaban.spi.StartupDependencyDetails;
import azkaban.spi.StartupDependencyFile;
import azkaban.spi.Storage;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DependencyStorage {
  private static final Logger log = LoggerFactory.getLogger(DependencyStorage.class);

  private final DatabaseOperator dbOperator;
  private final Storage storage;

  @Inject
  DependencyStorage(final DatabaseOperator dbOperator, final Storage storage) {
    this.storage = storage;
    this.dbOperator = dbOperator;
  }

  public Set<StartupDependencyDetails> getValidatedDependencies(final Set<StartupDependencyDetails> deps,
      final String validatorKey) throws SQLException {
    Map<String, StartupDependencyDetails> hashToDep = new HashMap<>();
    PreparedStatement stmnt = this.dbOperator.getDataSource().getConnection().prepareStatement(
        "select file_sha1 from startup_dependencies where validation_key = ? and file_sha1 in ("
            + makeStrWithQuestionMarks(deps.size()) + ")");

    stmnt.setString(1, validatorKey);

    int index = 2;
    for (StartupDependencyDetails d : deps) {
      stmnt.setString(index++, d.getSHA1());
      hashToDep.put(d.getSHA1(), d);
    }

    ResultSet rs = stmnt.executeQuery();

    Set<StartupDependencyDetails> validatedDependencies = new HashSet<>();
    while (rs.next()) {
      validatedDependencies.add(hashToDep.get(rs.getString(1)));
    }
    return validatedDependencies;
  }

  public void persistDependencies(final Set<StartupDependencyFile> depFiles, final String validationKey)
      throws SQLException, IOException {
    for (StartupDependencyFile f : depFiles) {
      persistDependency(f);
    }

    String[][] rowsToInsert = depFiles
        .stream()
        .map(f -> new String[]{f.getDetails().getSHA1(), validationKey})
        .toArray(String[][]::new);

    this.dbOperator.batch("insert ignore into startup_dependencies values (?, ?)", rowsToInsert);

    // Ensure all dependencies exist. If any of them don't, roll back the appropriate entry in the database
    // and set a flag to throw an error. We don't immediately throw the error through, go through all the
    // dependencies to make sure we roll back all dependencies that we've failed to persist before throwing
    // the error.
    boolean failedToPersist = false;
    for (StartupDependencyFile f : depFiles) {
      if (!this.storage.existsDependency(f.getDetails())) {
        failedToPersist = true;
        deleteRowsForDependency(f.getDetails());
      }
    }
    if (failedToPersist) {
      throw new IOException("Some dependencies failed to persist. DB changes have been rolled back.");
    }
  }

  private void persistDependency(final StartupDependencyFile f) throws IOException, SQLException {
    if (!this.storage.existsDependency(f.getDetails())) {
      try {
        this.storage.putDependency(f);
      } catch (Exception e) {
        log.error(String.format("Failed to persist dependency %s so deleting all "
          + "validation entries with file sha1 %s", f.getDetails().getFile(), f.getDetails().getSHA1()));
        deleteRowsForDependency(f.getDetails());
        throw e;
      }
    }
  }

  private void deleteRowsForDependency(final StartupDependencyDetails d) throws SQLException {
    this.dbOperator.update("delete from startup_dependencies where file_sha1 = ?", d.getSHA1());
  }

  private String makeStrWithQuestionMarks(final int num) {
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < num; i++) {
      builder.append("?,");
    }
    return builder.deleteCharAt(builder.length() - 1).toString();
  }
}
