package azkaban.utils;

import azkaban.project.StartupDependencyDetails;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.io.FileUtils;

import static com.google.common.base.Preconditions.*;


public class ThinArchiveUtils {
  public static File getStartupDependenciesFile(final File folder) {
    return new File(folder.getPath() + "/app-meta/startup-dependencies.json");
  }

  public static List<StartupDependencyDetails> parseStartupDependencies(final File f) throws IOException {
    final String rawJson = FileUtils.readFileToString(f);
    return ((HashMap<String, List<Map<String, String>>>)
        JSONUtils.parseJSONFromString(rawJson))
        .get("dependencies")
        .stream().map(StartupDependencyDetails::new)
        .collect(Collectors.toList());
  }

  public static void writeStartupDependencies(final File f,
      final List<StartupDependencyDetails> dependencies) throws IOException {
    Map<String, List<StartupDependencyDetails>> outputFormat = new HashMap<>();
    outputFormat.put("dependencies", dependencies);
    FileUtils.writeStringToFile(f, JSONUtils.toJSON(outputFormat));
  }

  public static String getArtifactoryUrlForDependency(StartupDependencyDetails d) {
    String[] coordinateParts = d.getIvyCoordinates().split(":");
    return "http://dev-artifactory.corp.linkedin.com:8081/artifactory/release/"
        + coordinateParts[0].replace(".", "/") + "/"
        + coordinateParts[1] + "/"
        + coordinateParts[2] + "/"
        + d.getFileName();
  }

  public static void validateDependencyHash(final File dependencyFile, final StartupDependencyDetails dependencyInfo)
      throws HashNotMatchException {
    try {
      final byte[] actualFileHash = HashUtils.SHA1.getHash(dependencyFile);
      if (!HashUtils.isSameHash(dependencyInfo.getSHA1(), actualFileHash)) {
        throw new HashNotMatchException(String.format("SHA1 Dependency hash check failed. File: %s Expected: %s Actual: %s",
            dependencyInfo.getFileName(),
            dependencyInfo.getSHA1(),
            HashUtils.bytesHashToString(actualFileHash)));
      }
    } catch (DecoderException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
