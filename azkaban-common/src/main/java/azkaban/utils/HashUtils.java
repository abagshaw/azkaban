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


/**
 * Helper class that will find the hashes for files.
 */

public enum HashUtils {
  MD5("MD5"),
  SHA1("SHA1");

  private String type;

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
    return isSameHash(Hex.decodeHex(a.toCharArray()), b);
  }

  public static boolean isSameHash(byte[] a, byte[] b) {
    return Arrays.equals(a, b);
  }

}
