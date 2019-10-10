package azkaban.utils;

import azkaban.spi.Dependency;
import azkaban.spi.Storage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.io.FileUtils;


public class ThinArchiveUtils {
  public static File getStartupDependenciesFile(final File projectFolder) {
    return new File(projectFolder.getPath() + "/app-meta/startup-dependencies.json");
  }

  public static File getDependencyFile(final File projectFolder, final Dependency d) {
    return new File(projectFolder, d.getDestination() + File.separator + d.getFile());
  }

  public static Set<Dependency> parseStartupDependencies(final File f) throws IOException {
    final String rawJson = FileUtils.readFileToString(f);
    return ((HashMap<String, Set<Map<String, String>>>)
        JSONUtils.parseJSONFromString(rawJson))
        .get("dependencies")
        .stream().map(Dependency::new)
        .collect(Collectors.toSet());
  }

  public static void writeStartupDependencies(final File f,
      final Set<Dependency> dependencies) throws IOException {
    Map<String, Set<Dependency>> outputFormat = new HashMap<>();
    outputFormat.put("dependencies", dependencies);
    FileUtils.writeStringToFile(f, JSONUtils.toJSON(outputFormat));
  }

  public static String convertIvyCoordinateToPath(final Dependency dep) {
    String[] coordinateParts = dep.getIvyCoordinates().split(":");
    return coordinateParts[0].replace(".", "/") + "/"
        + coordinateParts[1] + "/"
        + coordinateParts[2] + "/"
        + dep.getFile();
  }

  public static List<String> replaceLocalPathsWithStoragePaths(final File projectFolder,
      final List<String> localDependencies, final Props systemProps) {
    File startupDependenciesFile = getStartupDependenciesFile(projectFolder);
    if (!startupDependenciesFile.exists()) {
      // This is not a thin archive - so we can't do any replacing
      return localDependencies;
    }

    try {
      Set<Dependency> startupDeps = parseStartupDependencies(startupDependenciesFile);

      Map<String, Dependency> pathToDep = new HashMap<>();
      for (Dependency dep : startupDeps) {
        pathToDep.put(getDependencyFile(projectFolder, dep).getCanonicalPath(), dep);
      }

      List<String> finalDependencies = new ArrayList<>();
      for (String localDepPath : localDependencies) {
        final String localDepCanonicalPath = new File(localDepPath).getCanonicalPath();

        if (pathToDep.containsKey(localDepCanonicalPath)) {
          // This dependency was listed in startup-dependencies.json so we can replace its local filepath
          // with a storage path!
          String baseDependencyPath = systemProps.get(Storage.DEPENDENCY_STORAGE_PATH_PREFIX_PROP);
          if (baseDependencyPath.endsWith("/")) {
            baseDependencyPath = baseDependencyPath.substring(0, baseDependencyPath.length() - 1);
          }

          String pathToDependencyInStorage =
              baseDependencyPath + "/" + convertIvyCoordinateToPath(pathToDep.get(localDepCanonicalPath));

          finalDependencies.add(pathToDependencyInStorage);
        } else {
          // This dependency was not found in startup-dependencies.json so just keep it's original local filepath
          // entry
          finalDependencies.add(localDepPath);
        }
      }

      return finalDependencies;
    } catch (IOException e) {
      // If something goes wrong, swallow the error and just return the local dependencies list.
      return localDependencies;
    }
  }

  public static void validateDependencyHash(final File dependencyFile, final Dependency dependencyInfo)
      throws HashNotMatchException {
    try {
      final byte[] actualFileHash = HashUtils.SHA1.getHash(dependencyFile);
      if (!HashUtils.isSameHash(dependencyInfo.getSHA1(), actualFileHash)) {
        throw new HashNotMatchException(String.format("SHA1 Dependency hash check failed. File: %s Expected: %s Actual: %s",
            dependencyInfo.getFile(),
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
