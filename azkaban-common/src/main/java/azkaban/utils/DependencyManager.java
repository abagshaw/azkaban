package azkaban.utils;

import azkaban.db.DatabaseOperator;
import azkaban.spi.FileStatus;
import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.Storage;
import azkaban.spi.FileValidationStatus;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
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


public class DependencyManager {
  private static final Logger log = LoggerFactory.getLogger(DependencyManager.class);

  private final DatabaseOperator dbOperator;
  private final Storage storage;

  @Inject
  DependencyManager(final DatabaseOperator dbOperator, final Storage storage) {
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

  public Set<DependencyFile> persistDependencies(final Set<DependencyFile> depFiles) throws IOException {
    final Set<DependencyFile> persistedDeps = new HashSet<>();
    for (DependencyFile f : depFiles) {
      // If the file has a status of OPEN (it should never have a status of NON_EXISTANT) then we will not
      // add an entry in the DB because it's possible that the other process that is currently writing the
      // dependency fails and we want to ensure that the DB ONLY includes entries for verified dependencies
      // when they are GUARANTEED to be persisted to storage.
      FileStatus resultOfPersisting = persistDependency(f);
      if (resultOfPersisting == FileStatus.CLOSED) {
        // The dependency has a status of closed, so we are guaranteed that it persisted successfully to storage.
        persistedDeps.add(f);
      }
    }

    return persistedDeps;
  }

  private FileStatus persistDependency(final DependencyFile f) throws IOException {
    FileStatus status = this.storage.dependencyStatus(f);
    if (status == FileStatus.NON_EXISTANT) {
      try {
        this.storage.putDependency(f);
        status = FileStatus.CLOSED;
      } catch (FileAlreadyExistsException e) {
        // Looks like another process beat us to the race. It started writing the file before we could.
        // It's possible that the file completed writing the file, but it's also possible that the file is
        // still being written to. We will assume the worst case (the file is still being written to) and
        // return a status of OPEN so as not to persist this entry in the DB.
        status = FileStatus.OPEN;
      } catch (IOException e) {
        log.error("Error while attempting to persist dependency " + f.getFileName());
        throw e;
      }
    }
    return status;
  }

  private String makeStrWithQuestionMarks(final int num) {
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < num; i++) {
      builder.append("?,");
    }
    return builder.deleteCharAt(builder.length() - 1).toString();
  }
}
