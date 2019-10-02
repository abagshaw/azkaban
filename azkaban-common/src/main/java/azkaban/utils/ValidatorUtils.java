package azkaban.utils;

import azkaban.project.Project;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidatorManager;
import azkaban.project.validator.XmlValidatorManager;
import java.io.File;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ValidatorUtils {
  private static final Logger logger = LoggerFactory.getLogger(ValidatorUtils.class);

  public static Map<String, ValidationReport> validateProject(final Project project, final File folder, final Props prop) {
    // Reload XmlValidatorManager and create new object for each project verification because props
    // must be unique between validations.
    final ValidatorManager validatorManager = new XmlValidatorManager(prop);
    logger.info("Validating project " + project.getName()
        + " using the registered validators "
        + validatorManager.getValidatorsInfo().toString());
    return validatorManager.validate(project, folder);
  }
}
