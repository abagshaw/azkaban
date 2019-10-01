package azkaban.project;

import java.util.Map;
import java.util.Objects;


public class StartupDependencyDetails {
  private final String file;
  private final String destination;
  private final String type;
  private final String ivyCoordinates;
  private final String sha1;

  public StartupDependencyDetails(Map<String, String> m) {
    this.file = m.get("file");
    this.destination = m.get("destination");
    this.type = m.get("type");
    this.ivyCoordinates = m.get("ivyCoordinates");
    this.sha1 = m.get("sha1");
  }

  public String getFileName() { return file; }
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
    StartupDependencyDetails that = (StartupDependencyDetails) o;
    return file.equals(that.file) && type.equals(that.type) && ivyCoordinates.equals(that.ivyCoordinates)
        && sha1.equals(that.sha1);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sha1);
  }
}
