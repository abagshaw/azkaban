package azkaban.utils;

import azkaban.project.StartupDependency;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;


public class ThinArchiveUtils {
  public static File getStartupDependenciesFile(final File folder) {
    return new File(folder.getPath() + "/app-meta/startup-dependencies.json");
  }

  public static List<StartupDependency> parseStartupDependencies(final File startupDependencies) throws IOException {
    final String rawJson = FileUtils.readFileToString(startupDependencies);
    return ((HashMap<String, List<Map<String, String>>>)
        JSONUtils.parseJSONFromString(rawJson))
        .get("dependencies")
        .stream().map(StartupDependency::fromMap)
        .collect(Collectors.toList());
  }
}
