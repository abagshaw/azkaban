package azkaban.utils;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FilenameUtils;


public class StorageUtils {
  public static String createTargetProjectFilename(final int projectId, byte[] hash) {
    return String.format("%s-%s.zip",
        String.valueOf(projectId),
        new String(Hex.encodeHex(hash))
    );
  }

  public static String createTargetDependencyFilename(final String name, final String sha1) {
    // some-interesting-lib-1.0.0.jar with sha1 2ac9fb2370f20df15a59438282c3ce3ca04b8d2a
    // will get name some-interesting-lib-1.0.0-2ac9fb2370f20df15a59438282c3ce3ca04b8d2a.jar
    return String.format("%s-%s.%s",
        FilenameUtils.removeExtension(name),
        sha1,
        FilenameUtils.getExtension(name)
    );
  }
}
