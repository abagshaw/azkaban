package azkaban.spi;

import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.InvalidHashException;
import org.junit.Test;

import static org.junit.Assert.*;


public class DependencyTest {
  @Test
  public void testCreateValidDependency() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    Dependency newDep =
        new Dependency(depA.getFileName(), depA.getDestination(), depA.getType(), depA.getIvyCoordinates(), depA.getSHA1());

    assertEquals(depA, newDep);
  }

  @Test
  public void testCopyDependency() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    Dependency newDep = depA.copy();

    assertEquals(depA, newDep);
  }

  @Test(expected = InvalidHashException.class)
  public void testInvalidHash() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    new Dependency(
            depA.getFileName(),
            depA.getDestination(),
            depA.getType(),
            depA.getIvyCoordinates(),
            "uh oh, I'm not a hash :(");
  }
}
