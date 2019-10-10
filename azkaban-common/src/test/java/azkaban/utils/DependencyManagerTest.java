package azkaban.utils;

import azkaban.db.AzkabanDataSource;
import azkaban.db.DatabaseOperator;
import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.FileStatus;
import azkaban.spi.FileValidationStatus;
import azkaban.spi.Storage;
import azkaban.test.executions.ThinArchiveTestSampleData;
import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;


public class DependencyManagerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  public final String VALIDATION_KEY = "123";

  public DatabaseOperator dbOperator;
  public Storage storage;
  public DependencyManager dependencyManager;

  public Dependency DEP_A;
  public Dependency DEP_B;
  public Dependency DEP_C;

  @Before
  public void setup() throws Exception {
    this.dbOperator = mock(DatabaseOperator.class);
    this.storage = mock(Storage.class);

    this.dependencyManager = new DependencyManager(this.dbOperator, this.storage);

    DEP_A = ThinArchiveTestSampleData.getDepA();
    DEP_B = ThinArchiveTestSampleData.getDepB();
    DEP_C = ThinArchiveTestSampleData.getDepC();
  }

  @Test
  public void testGetValidationStatuses() throws Exception {
    // This test isn't very good and does NOT verify anything about the correctness of the SQL query
    // in order to avoid brittleness.
    AzkabanDataSource dataSource = mock(AzkabanDataSource.class);
    Connection connection = mock(Connection.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(this.dbOperator.getDataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(rs);

    // Also for some reason ResultSets return the first result at index at 1 so we set pad this with a
    // null at the beginning.
    Object[][] results = new Object[][] {
        null,
        {DEP_A.getSHA1(), FileValidationStatus.REMOVED.getValue()},
        {DEP_C.getSHA1(), FileValidationStatus.VALID.getValue()}
    };

    final AtomicInteger currResultIndex = new AtomicInteger();
    doAnswer((Answer<Boolean>) invocation -> currResultIndex.getAndIncrement() < results.length).when(rs).next();
    doAnswer((Answer<String>) invocation -> (String) results[currResultIndex.get()][0]).when(rs).getString(1);
    doAnswer((Answer<Integer>) invocation -> (Integer) results[currResultIndex.get()][1]).when(rs).getInt(2);

    Map<Dependency, FileValidationStatus> expectedResult = new HashMap();
    expectedResult.put(DEP_A, FileValidationStatus.REMOVED);
    expectedResult.put(DEP_B, FileValidationStatus.NEW);
    expectedResult.put(DEP_C, FileValidationStatus.VALID);

    // Assert that depB is the only dependency returned as validated
    assertEquals(expectedResult,
        this.dependencyManager.getValidationStatuses(ThinArchiveTestSampleData.getDepSetABC(), VALIDATION_KEY));
  }

  @Test
  public void testPersistTwoDependenciesNotInStorage() throws Exception {
    // Indicate that the jars are not already in storage
    when(this.storage.dependencyStatus(DEP_A)).thenReturn(FileStatus.NON_EXISTANT);
    when(this.storage.dependencyStatus(DEP_B)).thenReturn(FileStatus.NON_EXISTANT);

    Set<DependencyFile> depFiles = new HashSet();
    DependencyFile depAFile = new DependencyFile(TEMP_DIR.newFile(DEP_A.getFileName()), DEP_A);
    DependencyFile depBFile = new DependencyFile(TEMP_DIR.newFile(DEP_B.getFileName()), DEP_B);
    depFiles.add(depAFile);
    depFiles.add(depBFile);

    Set<DependencyFile> guaranteedPersistedDeps = depFiles;

    // We expect both dependencies to be guaranteed persisted so both should be returned
    assertEquals(guaranteedPersistedDeps, this.dependencyManager.persistDependencies(depFiles));

    verify(this.storage).putDependency(depAFile);
    verify(this.storage).putDependency(depBFile);
  }

  @Test
  public void testPersistTwoDependenciesAlreadyInStorage() throws Exception {
    // Indicate that one jar is CLOSED (exists and not being written to) and another is OPEN (exists and
    // is currently being written to. It should NOT attempt to persist EITHER of these dependencies.
    when(this.storage.dependencyStatus(DEP_A)).thenReturn(FileStatus.CLOSED);
    when(this.storage.dependencyStatus(DEP_B)).thenReturn(FileStatus.OPEN);

    Set<DependencyFile> depFiles = new HashSet();
    DependencyFile depAFile = new DependencyFile(TEMP_DIR.newFile(DEP_A.getFileName()), DEP_A);
    DependencyFile depBFile = new DependencyFile(TEMP_DIR.newFile(DEP_B.getFileName()), DEP_B);
    depFiles.add(depAFile);
    depFiles.add(depBFile);

    Set<DependencyFile> guaranteedPersistedDeps = new HashSet<>();
    guaranteedPersistedDeps.add(depAFile);

    // We ONLY expect the CLOSED dependency to be returned as guaranteed persisted. The OPEN one we aren't 100%
    // will be successfully persisted so we can't rely on that.
    assertEquals(depFiles, this.dependencyManager.persistDependencies(depFiles));

    verify(this.storage, never()).putDependency(any());
  }

  @Test
  public void testPersistDependencyRaceCondition() throws Exception {
    // Indicate that the jar is not in storage
    when(this.storage.dependencyStatus(DEP_A)).thenReturn(FileStatus.NON_EXISTANT);

    Set<DependencyFile> depFiles = new HashSet();
    DependencyFile depAFile = new DependencyFile(TEMP_DIR.newFile(DEP_A.getFileName()), DEP_A);
    DependencyFile depBFile = new DependencyFile(TEMP_DIR.newFile(DEP_B.getFileName()), DEP_B);
    depFiles.add(depAFile);
    depFiles.add(depBFile);

    // When the persist method attempts to actually write the dependency, throw a FileAlreadyExistsException
    // to indicate another process beat us to the race and started writing to the file AFTER we checked
    // for its existence and found it did not exist.
    doThrow(new FileAlreadyExistsException("Uh oh :(")).when(this.storage).putDependency(depAFile);

    Set<DependencyFile> guaranteedPersistedDeps = new HashSet<>();

    // We expect NO dependencies to be returned as guaranteed persisted because we hit a race condition
    // and to guarantee DB consistency with files in storage, we will defer writing an entry for this
    // dependency in the DB until the next project upload with this dependency.
    assertEquals(guaranteedPersistedDeps, this.dependencyManager.persistDependencies(depFiles));

    verify(this.storage).putDependency(depAFile);
  }

  @Test
  public void testPersistDependencyAlreadyInDB() throws Exception {
    // Indicate that the jar IS already in storage
    when(this.storage.existsDependency(ThinArchiveTestSampleData.getDepA())).thenReturn(true);

    // 1062 is the MySQL error code for error inserting due to duplicate primary key.
    SQLException duplicatePrimaryKey = new SQLException(null, null, 1062);
    when(this.dbOperator.update(Mockito.any())).thenThrow(duplicatePrimaryKey);

    File someFile = TEMP_DIR.newFile(ThinArchiveTestSampleData.getDepA().getFile());
    // The SQLException should be swallowed because even though we couldn't insert, we know that
    // the row already exists.
    this.dependencyManager.persistDependency(someFile, ThinArchiveTestSampleData.getDepA(), VALIDATION_KEY);

    verify(this.dbOperator).update(Mockito.anyString(), any());
  }

  @Test(expected = Exception.class)
  public void testPersistDependencyDBError() throws Exception {
    // Indicate that the jar IS already in storage
    when(this.storage.existsDependency(ThinArchiveTestSampleData.getDepA())).thenReturn(true);

    // I have no idea what error code 8271 does, but it should trigger an exception that isn't swallowed!
    SQLException duplicatePrimaryKey = new SQLException(null, null, 8271);
    doThrow(duplicatePrimaryKey).when(this.dbOperator.update(Mockito.any()));

    File someFile = TEMP_DIR.newFile(ThinArchiveTestSampleData.getDepA().getFile());
    // We should get an error thrown because of a failed SQL insert.
    this.dependencyManager.persistDependency(someFile, ThinArchiveTestSampleData.getDepA(), VALIDATION_KEY);
  }
}
