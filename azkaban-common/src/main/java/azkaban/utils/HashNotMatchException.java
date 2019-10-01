package azkaban.utils;

public class HashNotMatchException extends RuntimeException {
  public HashNotMatchException() {
    super();
  }

  public HashNotMatchException(final String s) {
    super(s);
  }

  public HashNotMatchException(final String s, final Exception e) {
    super(s, e);
  }
}
