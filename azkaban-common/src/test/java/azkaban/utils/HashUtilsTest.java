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

  private static final String SAMPLE_STR = "abcd123";
  private static final String SAMPLE_STR_MD5 = "79CFEB94595DE33B3326C06AB1C7DBDA";
  private static final String SAMPLE_STR_SHA1 = "7C3607B8E61BCF1944E9E8503A660F21F4B6F3F1";

  @Test
  public void MD5onFile() throws Exception {
    assertThat(HashUtils.MD5.getHash(ZIP_FILE), is(ZIP_MD5_BYTES));
  }

  @Test
  public void SHA1onFile() throws Exception {
    assertThat(HashUtils.SHA1.getHash(ZIP_FILE), is(ZIP_SHA1_BYTES));
  }

  @Test
  public void MD5onString() throws Exception {
    assertEquals(
        HashUtils.bytesHashToString(HashUtils.MD5.getHash(SAMPLE_STR)).toUpperCase(),
        SAMPLE_STR_MD5);
  }

  @Test
  public void SHA1onString() throws Exception {
    assertEquals(
        HashUtils.bytesHashToString(HashUtils.SHA1.getHash(SAMPLE_STR)).toUpperCase(),
        SAMPLE_STR_SHA1);
  }

  @Test
  public void isSameHash() throws Exception {
    assertTrue(HashUtils.isSameHash(ZIP_MD5_BASE64_STRING, ZIP_MD5_BYTES));
    assertTrue(HashUtils.isSameHash(ZIP_SHA1_BASE64_STRING, ZIP_SHA1_BYTES));
  }

  @Test
  public void stringToBytesToString() throws Exception {
    assertEquals(ZIP_MD5_BASE64_STRING.toLowerCase(),
        HashUtils.bytesHashToString(HashUtils.stringHashToBytes(ZIP_MD5_BASE64_STRING)));
  }
}
