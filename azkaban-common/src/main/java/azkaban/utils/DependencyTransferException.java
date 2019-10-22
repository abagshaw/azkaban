package azkaban.utils;

public class DependencyTransferException extends RuntimeException {
  public DependencyTransferException() {
    super();
  }

  public DependencyTransferException(final String s) {
    super(s);
  }

  public DependencyTransferException(final String s, final Throwable e) {
    super(s, e);
  }
}
