package azkaban.test.executions;

import azkaban.project.StartupDependencyDetails;
import java.util.Arrays;
import java.util.List;


public class ThinArchiveTestSampleData {
  public static List getDepList() { return Arrays.asList(getDepA(), getDepB()); }

  public static String getRawJSON() {
    return "{" +
        "    \"dependencies\": [" +
        "        {" +
        "            \"sha1\": \"131BD316A77423E6B80D93262B576C139C72B4C3\"," +
        "            \"file\": \"a.jar\"," +
        "            \"destination\": \"lib\"," +
        "            \"type\": \"jar\"," +
        "            \"ivyCoordinates\": \"com.linkedin.test:testera:1.0.1\"" +
        "        }," +
        "        {" +
        "            \"sha1\": \"9461919846E1E7C8FC74FEE95AA6AC74993BE71E\"," +
        "            \"file\": \"b.jar\"," +
        "            \"destination\": \"lib\"," +
        "            \"type\": \"jar\"," +
        "            \"ivyCoordinates\": \"com.linkedin.test:testerb:1.0.1\"" +
        "        }" +
        "    ]" +
        "}";
  }

  public static String getDepAContent() { return "blahblah12"; }
  public static StartupDependencyDetails getDepA() {
    return new StartupDependencyDetails(
      "a.jar",
      "lib",
      "jar",
      "com.linkedin.test:testera:1.0.1",
      "131BD316A77423E6B80D93262B576C139C72B4C3");
  }

  public static String getDepBContent() { return "ladedah83"; }
  public static StartupDependencyDetails getDepB() {
    return new StartupDependencyDetails(
      "b.jar",
      "lib",
      "jar",
      "com.linkedin.test:testerb:1.0.1",
      "9461919846E1E7C8FC74FEE95AA6AC74993BE71E");
  }
}
