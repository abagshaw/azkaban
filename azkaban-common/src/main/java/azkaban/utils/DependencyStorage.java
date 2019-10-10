package azkaban.utils;

import azkaban.db.DatabaseOperator;
import azkaban.spi.FileStatus;
import azkaban.spi.StartupDependencyDetails;
import azkaban.spi.StartupDependencyFile;
import azkaban.spi.Storage;
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
        "select file_sha1 from validated_dependencies where validation_key = ? and file_sha1 in ("
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
    final Set<StartupDependencyFile> persistedDeps = new HashSet<>();
    for (StartupDependencyFile f : depFiles) {
      // If the file has a status of OPEN (it should never have a status of NON_EXISTANT) then we will not
      // add an entry in the DB because it's possible that the other process that is currently writing the
      // dependency fails and we want to ensure that the DB ONLY includes entries for verified dependencies
      // when they are GUARANTEED to be persisted to storage.
      if (persistDependency(f) == FileStatus.CLOSED) {
        // The dependency has a status of closed, so we are guaranteed that it persisted successfully to storage.
        persistedDeps.add(f);
      }
    }

    // Prepare to add entries in the validated_dependencies table for each dependency that we are 100% sure
    // has been successfully persisted to storage.
    String[][] rowsToInsert = persistedDeps
        .stream()
        .map(f -> new String[]{f.getDetails().getSHA1(), validationKey})
        .toArray(String[][]::new);

    this.dbOperator.batch("insert ignore into validated_dependencies values (?, ?)", rowsToInsert);
  }

  private FileStatus persistDependency(final StartupDependencyFile f) throws IOException {
    FileStatus status = this.storage.dependencyStatus(f.getDetails());
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
        log.error("Error while attempting to persist dependency " + f.getDetails().getFile());
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
