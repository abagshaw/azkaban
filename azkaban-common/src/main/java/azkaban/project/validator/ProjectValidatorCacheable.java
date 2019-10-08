package azkaban.project.validator;

import azkaban.project.Project;
import azkaban.utils.Props;
import java.io.File;


/**
 * Interface to be implemented by plugins which are to be registered with Azkaban as project
 * validators that validate a project before uploaded into Azkaban.
 */
public interface ProjectValidatorCacheable extends ProjectValidator {
  /**
   * Get a hash representing a context state for a project. Adding or removing JARs should
   * not change this state. The state should only be based on text files. Two different projects
   * for which the validator returns the same cacheKey should have IDENTICAL validation results
   * for a given jar. I.e. if coollib-1.0.0.jar is included in Proj1 and is also in Proj2, so long
   * as the validator returns the same cacheKey for both Proj1 and Proj2 it should also return the
   * same validation result for the coollib-1.0.0.jar present in both projects.
   */
  String getCacheKey(Project proj, File projectDir, Props additionalProps);
}
