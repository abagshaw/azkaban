package azkaban.project;

import java.util.Map;
import java.util.Objects;


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

  public String getFile() { return file; }
  public String getDestination() { return destination; }
  public String getType() { return type; }
  public String getIvyCoordinates() { return ivyCoordinates; }
  public String getSHA1() { return sha1; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StartupDependency that = (StartupDependency) o;
    return file.equals(that.file) && type.equals(that.type) && ivyCoordinates.equals(that.ivyCoordinates)
        && sha1.equals(that.sha1);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sha1);
  }
}
