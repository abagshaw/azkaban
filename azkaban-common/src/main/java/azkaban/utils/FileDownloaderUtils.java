package azkaban.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;


public class FileDownloaderUtils {
  public static void downloadToFile(File destination, URL fromURL) throws IOException {
    FileOutputStream fileOS = new FileOutputStream(destination, false);
    FileChannel writeChannel = fileOS.getChannel();
    ReadableByteChannel readChannel = Channels.newChannel(fromURL.openStream());
    writeChannel.transferFrom(readChannel, 0, Long.MAX_VALUE);
  }
}
