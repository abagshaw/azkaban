package azkaban.test.executions;

import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.mockito.ArgumentMatcher;

import static org.mockito.ArgumentMatchers.*;

// Custom mockito argument matcher to help with matching Dependency with DependencyFile
class DependencyMatcher implements ArgumentMatcher<DependencyFile> {
  private Dependency dep;
  public DependencyMatcher(Dependency d) {
    this.dep = d;
  }

  @Override
  public boolean matches(DependencyFile depFile) {
    try {
      return dep.getFileName().equals(((Dependency) depFile).getFileName());
    } catch (Exception e) {
      return false;
    }
  }
}


public class ThinArchiveTestUtils {
  public static DependencyFile depEq(Dependency dep) {
    return argThat(new DependencyMatcher(dep));
  }

  public static Set getDepSetA() { return new HashSet(Arrays.asList(getDepA())); }
  public static Set getDepSetB() { return new HashSet(Arrays.asList(getDepB())); }
  public static Set getDepSetAB() { return new HashSet(Arrays.asList(getDepA(), getDepB())); }
  public static Set getDepSetABC() { return new HashSet(Arrays.asList(getDepA(), getDepB(), getDepC())); }

  public static String getRawJSONDepA() {
    return "{" +
        "    \"dependencies\": [" +
                depAJSONBlock() +
        "    ]" +
        "}";
  }

  public static String getRawJSONDepB() {
    return "{" +
        "    \"dependencies\": [" +
                depBJSONBlock() +
        "    ]" +
        "}";
  }

  public static String getRawJSONDepC() {
    return "{" +
        "    \"dependencies\": [" +
                depCJSONBlock() +
        "    ]" +
        "}";
  }

  public static String getRawJSONDepsAB() {
    return "{" +
        "    \"dependencies\": [" +
                depAJSONBlock() + "," +
                depBJSONBlock() +
        "    ]" +
        "}";
  }

  public static String getRawJSONDepsABC() {
    return "{" +
        "    \"dependencies\": [" +
                depAJSONBlock() + "," +
                depBJSONBlock() + "," +
                depCJSONBlock() +
        "    ]" +
        "}";
  }

  private static String depAJSONBlock() {
    return "{" +
        "    \"sha1\": \"131BD316A77423E6B80D93262B576C139C72B4C3\"," +
        "    \"file\": \"aaaa.jar\"," +
        "    \"destination\": \"lib\"," +
        "    \"type\": \"jar\"," +
        "    \"ivyCoordinates\": \"com.linkedin.test:testeraaaa:1.0.1\"" +
        "}";
  }

  private static String depBJSONBlock() {
    return "{" +
        "    \"sha1\": \"9461919846E1E7C8FC74FEE95AA6AC74993BE71E\"," +
        "    \"file\": \"bbbb.jar\"," +
        "    \"destination\": \"lib\"," +
        "    \"type\": \"jar\"," +
        "    \"ivyCoordinates\": \"com.linkedin.test:testerbbbb:1.0.1\"" +
        "}";
  }

  private static String depCJSONBlock() {
    return "{" +
        "    \"sha1\": \"F873F39163F5B43DBF1FEE63CBCE284074896221\"," +
        "    \"file\": \"cccc.jar\"," +
        "    \"destination\": \"lib\"," +
        "    \"type\": \"jar\"," +
        "    \"ivyCoordinates\": \"com.linkedin.test:testercccc:1.0.1\"" +
        "}";
  }

  public static String getDepAContent() { return "blahblah12"; }
  public static String getDepAPath() {
    return "com/linkedin/test/testeraaaa/1.0.1/aaaa.jar";
  }
  public static Dependency getDepA() {
    return new Dependency(
      "aaaa.jar",
      "lib",
      "jar",
      "com.linkedin.test:testeraaaa:1.0.1",
      "131BD316A77423E6B80D93262B576C139C72B4C3");
  }

  public static String getDepBContent() { return "ladedah83"; }
  public static String getDepBPath() {
    return "com/linkedin/test/testerbbbb/1.0.1/bbbb.jar";
  }
  public static Dependency getDepB() {
    return new Dependency(
      "bbbb.jar",
      "lib",
      "jar",
      "com.linkedin.test:testerbbbb:1.0.1",
      "9461919846E1E7C8FC74FEE95AA6AC74993BE71E");
  }

  public static String getDepCContent() { return "myprecious"; }
  public static String getDepCPath() {
    return "com/linkedin/test/testercccc/1.0.1/cccc.jar";
  }
  public static Dependency getDepC() {
    return new Dependency(
        "cccc.jar",
        "lib",
        "jar",
        "com.linkedin.test:testercccc:1.0.1",
        "F873F39163F5B43DBF1FEE63CBCE284074896221");
  }
}
