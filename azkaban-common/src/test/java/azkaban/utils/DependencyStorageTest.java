package azkaban.utils;

import azkaban.db.DatabaseOperator;
import azkaban.spi.Storage;
import azkaban.test.executions.ThinArchiveTestSampleData;
import java.io.File;
import java.sql.SQLException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;


public class DependencyStorageTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  public final String VALIDATION_KEY = "123";

  public DatabaseOperator dbOperator;
  public Storage storage;
  public DependencyStorage dependencyStorage;

  @Before
  public void setup() throws Exception {
    this.dbOperator = mock(DatabaseOperator.class);
    this.storage = mock(Storage.class);

    this.dependencyStorage = new DependencyStorage(this.dbOperator, this.storage);
  }

  @Test
  public void testDependencyExistsAndIsValidated() throws Exception {
    // This isn't a very good test, but to avoid making it brittle this is the best I think we can do
    this.dependencyStorage.dependencyExistsAndIsValidated(ThinArchiveTestSampleData.getDepA(), VALIDATION_KEY);
    verify(this.dbOperator).query(Mockito.anyString(), any());
  }

  @Test
  public void testPersistDependencyNotInStorage() throws Exception {
    // Indicate that the jar is not already in storage
    when(this.storage.existsDependency(ThinArchiveTestSampleData.getDepA())).thenReturn(false);

    File someFile = TEMP_DIR.newFile(ThinArchiveTestSampleData.getDepA().getFile());
    this.dependencyStorage.persistDependency(someFile, ThinArchiveTestSampleData.getDepA(), VALIDATION_KEY);

    verify(this.storage).putDependency(someFile, ThinArchiveTestSampleData.getDepA());
    verify(this.dbOperator).update(Mockito.anyString(), any());
  }

  @Test
  public void testPersistDependencyAlreadyInStorage() throws Exception {
    // Indicate that the jar IS already in storage
    when(this.storage.existsDependency(ThinArchiveTestSampleData.getDepA())).thenReturn(true);

    File someFile = TEMP_DIR.newFile(ThinArchiveTestSampleData.getDepA().getFile());
    this.dependencyStorage.persistDependency(someFile, ThinArchiveTestSampleData.getDepA(), VALIDATION_KEY);

    // The dependency is already in storage, so we should not try to persist it to storage!
    verify(this.storage, Mockito.never()).putDependency(someFile, ThinArchiveTestSampleData.getDepA());
    verify(this.dbOperator).update(Mockito.anyString(), any());
  }

  @Test
  public void testPersistDependencyAlreadyInDB() throws Exception {
    // Indicate that the jar IS already in storage
    when(this.storage.existsDependency(ThinArchiveTestSampleData.getDepA())).thenReturn(true);

    // 1062 is the MySQL error code for error inserting due to duplicate primary key.
    SQLException duplicatePrimaryKey = new SQLException(null, null, 1062);
    doThrow(duplicatePrimaryKey).when(this.dbOperator.update(Mockito.any()));

    File someFile = TEMP_DIR.newFile(ThinArchiveTestSampleData.getDepA().getFile());
    // The SQLException should be swallowed because even though we couldn't insert, we know that
    // the row already exists.
    this.dependencyStorage.persistDependency(someFile, ThinArchiveTestSampleData.getDepA(), VALIDATION_KEY);

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
    this.dependencyStorage.persistDependency(someFile, ThinArchiveTestSampleData.getDepA(), VALIDATION_KEY);
  }
}
