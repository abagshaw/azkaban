package azkaban.utils;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import java.io.File;
import org.junit.Test;

public class HashUtilsTest {
  private static final File ZIP_FILE = new File("src/test/resources/sample_flow_01.zip");

  private static final byte[] ZIP_MD5_BYTES =
      new byte[]{-59, -26, 22, -50, -80, -101, -57, -121, -27, 46, -71, -101, -85, -115, 42, -116};
  private static final String ZIP_MD5_BASE64_STRING = "C5E616CEB09BC787E52EB99BAB8D2A8C";

  private static final byte[] ZIP_SHA1_BYTES =
      new byte[]{-119, 78, -53, 80, -34, -95, 32, -36, -103, -72, 123, 1, -10, -67, 63, -45, -47, -87, 82, 111};
  private static final String ZIP_SHA1_BASE64_STRING = "894ECB50DEA120DC99B87B01F6BD3FD3D1A9526F";

  @Test
  public void MD5() throws Exception {
    assertThat(HashUtils.MD5.getHash(ZIP_FILE), is(ZIP_MD5_BYTES));
    assertTrue(HashUtils.isSameHash(ZIP_MD5_BASE64_STRING, ZIP_MD5_BYTES));
  }

  @Test
  public void SHA1() throws Exception {
    assertThat(HashUtils.SHA1.getHash(ZIP_FILE), is(ZIP_SHA1_BYTES));
    assertTrue(HashUtils.isSameHash(ZIP_SHA1_BASE64_STRING, ZIP_SHA1_BYTES));
  }

}
