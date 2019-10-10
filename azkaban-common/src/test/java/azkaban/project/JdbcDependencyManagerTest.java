package azkaban.project;

import azkaban.db.AzkabanDataSource;
import azkaban.db.DatabaseOperator;
import azkaban.project.JdbcDependencyManager;
import azkaban.spi.Dependency;
import azkaban.spi.FileValidationStatus;
import azkaban.spi.Storage;
import azkaban.test.executions.ThinArchiveTestSampleData;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;


public class JdbcDependencyManagerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  public final String VALIDATION_KEY = "123";

  public DatabaseOperator dbOperator;
  public Storage storage;
  public JdbcDependencyManager jdbcDependencyManager;

  public Dependency DEP_A;
  public Dependency DEP_B;
  public Dependency DEP_C;

  @Before
  public void setup() {
    this.dbOperator = mock(DatabaseOperator.class);
    this.storage = mock(Storage.class);

    this.jdbcDependencyManager = new JdbcDependencyManager(this.dbOperator, this.storage);

    DEP_A = ThinArchiveTestSampleData.getDepA();
    DEP_B = ThinArchiveTestSampleData.getDepB();
    DEP_C = ThinArchiveTestSampleData.getDepC();
  }

  @Test
  public void testGetValidationStatuses() throws Exception {
    // This test isn't great and does NOT verify anything about the correctness of the SQL query
    // in order to avoid brittleness...although it's actually still pretty brittle given how we have to
    // anticipate exactly how the method creates a new PreparedStatement in order to mock it.
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
        this.jdbcDependencyManager.getValidationStatuses(ThinArchiveTestSampleData.getDepSetABC(), VALIDATION_KEY));
  }

  @Test
  public void testUpdateValidationStatuses() throws Exception {
    // This is another VERY WEAK TEST, that basically only verifies that the function runs without exceptions
    // and calls dbOperator.batch() but does not verify anything being passed to batch (i.e. the correctness
    // of the SQL query) so as not to make this test too brittle.
    Map<Dependency, FileValidationStatus> inputStatuses = new HashMap();
    inputStatuses.put(DEP_A, FileValidationStatus.REMOVED);
    inputStatuses.put(DEP_C, FileValidationStatus.VALID);

    verify(this.dbOperator).batch(anyString(), any());
  }
}
