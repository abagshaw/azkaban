package azkaban.spi;

import java.io.File;

public class StartupDependencyFile {
  private final File file;
  private final StartupDependencyDetails details;

  public StartupDependencyFile(File f, StartupDependencyDetails sd) {
    this.file = f;
    this.details = sd;
  }

  public File getFile() { return file; }
  public StartupDependencyDetails getDetails() { return details; }
}
