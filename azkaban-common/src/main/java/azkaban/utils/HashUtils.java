/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;


/**
 * Helper class that can find a hash for a file or string.
 */

public enum HashUtils {
  MD5("MD5"),
  SHA1("SHA1");

  private final String type;

  private static final int BYTE_BUFFER_SIZE = 1024;

  HashUtils(String type) {
    this.type = type;
  }

  public String getName() {
    return type;
  }

  private MessageDigest getDigest() {
    MessageDigest digest = null;
    try {
      digest = MessageDigest.getInstance(getName());
    } catch (final NoSuchAlgorithmException e) {
      // Should never get here.
    }
    return digest;
  }

  public byte[] getHash(final String str) {
    final MessageDigest digest = getDigest();
    digest.update(str.getBytes());
    return digest.digest();
  }

  public byte[] getHash(final File file) throws IOException {
    final MessageDigest digest = getDigest();

    final FileInputStream fStream = new FileInputStream(file);
    final BufferedInputStream bStream = new BufferedInputStream(fStream);
    final DigestInputStream blobStream = new DigestInputStream(bStream, digest);

    final byte[] buffer = new byte[BYTE_BUFFER_SIZE];

    int num = 0;
    do {
      num = blobStream.read(buffer);
    } while (num > 0);

    bStream.close();

    return digest.digest();
  }

  public static boolean isSameHash(String a, byte[] b) throws DecoderException {
    return isSameHash(stringHashToBytes(a), b);
  }

  public static boolean isSameHash(byte[] a, byte[] b) {
    return Arrays.equals(a, b);
  }

  public static byte[] stringHashToBytes(String a) throws DecoderException {
    return Hex.decodeHex(a.toCharArray());
  }

  public static String bytesHashToString(byte[] a) {
    return String.valueOf(Hex.encodeHex(a)).toLowerCase();
  }
}
