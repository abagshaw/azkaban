package azkaban.spi;

import java.util.Map;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonProperty;


public class Dependency {
  private final String fileName;
  private final String destination;
  private final String type;
  private final String ivyCoordinates;
  private final String sha1;

  public Dependency(String fileName, String destination, String type, String ivyCoordinates, String sha1) {
    this.fileName = fileName;
    this.destination = destination;
    this.type = type;
    this.ivyCoordinates = ivyCoordinates;
    this.sha1 = sha1;
  }

  public Dependency(Map<String, String> m) {
    this(m.get("file"), m.get("destination"), m.get("type"), m.get("ivyCoordinates"), m.get("sha1"));
  }

  @JsonProperty("file")
  public String getFileName() { return fileName; }
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
    Dependency that = (Dependency) o;
    return fileName.equals(that.fileName) && type.equals(that.type) && ivyCoordinates.equals(that.ivyCoordinates)
        && sha1.equals(that.sha1);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sha1);
  }
}
