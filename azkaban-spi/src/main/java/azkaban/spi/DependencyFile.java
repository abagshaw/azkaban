package azkaban.spi;

import java.io.File;

public class DependencyFile extends Dependency {
  private File file;

  public DependencyFile(File f, Dependency d) {
    super(d.getFileName(), d.getDestination(), d.getType(), d.getIvyCoordinates(), d.getSHA1());
    this.file = f;
  }

  public File getFile() { return this.file; }
  public void setFile(File file) { this.file = file; }
}
