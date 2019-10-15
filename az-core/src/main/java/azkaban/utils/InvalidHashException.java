package azkaban.utils;

public class InvalidHashException extends Exception {
  public InvalidHashException() {
    super();
  }

  public InvalidHashException(final String s) {
    super(s);
  }

  public InvalidHashException(final String s, final Exception e) {
    super(s, e);
  }
}
