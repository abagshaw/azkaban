package azkaban.project;

import java.util.Map;


public class StartupDependency {
  public String file;
  public String destination;
  public String type;
  public String ivyCoordinates;
  public String sha1;

  public static StartupDependency fromMap(Map<String, String> m) {
    StartupDependency s = new StartupDependency();
    s.file = m.get("file");
    s.destination = m.get("destination");
    s.type = m.get("type");
    s.ivyCoordinates = m.get("ivyCoordinates");
    s.sha1 = m.get("sha1");

    return s;
  }
}
