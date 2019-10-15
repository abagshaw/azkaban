package azkaban.spi;

import azkaban.utils.InvalidHashException;
import java.io.File;
import java.util.Objects;


public class DependencyFile extends Dependency {
  private File file;

  // NOTE: This should NEVER throw InvalidHashException because the input dependency
  // must have already had its cache validated upon instantiation
  public DependencyFile(File f, Dependency d) throws InvalidHashException {
    super(d.getFileName(), d.getDestination(), d.getType(), d.getIvyCoordinates(), d.getSHA1());
    this.file = f;
  }

  public File getFile() { return this.file; }
  public void setFile(File file) { this.file = file; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DependencyFile that = (DependencyFile) o;
    return Objects.equals(file, that.file);
  }
}
