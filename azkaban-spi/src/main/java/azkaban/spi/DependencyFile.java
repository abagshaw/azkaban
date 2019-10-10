package azkaban.spi;

import java.io.File;

public class DependencyFile extends Dependency {
  private File file;

  public DependencyFile(File f, String fileName, String destination, String type, String ivyCoordinates, String sha1) {
    super(fileName, destination, type, ivyCoordinates, sha1);
    this.file = f;
  }

  public File getFile() { return this.file; }
  public void setFile(File file) { this.file = file; }
}
