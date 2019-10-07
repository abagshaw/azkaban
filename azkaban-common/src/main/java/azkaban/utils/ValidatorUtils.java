package azkaban.utils;

import azkaban.project.Project;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidatorManager;
import azkaban.project.validator.XmlValidatorManager;
import java.io.File;
import java.util.Map;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ValidatorUtils {
  private static final Logger logger = LoggerFactory.getLogger(ValidatorUtils.class);

  private final ValidatorManager validatorManager;

  final Props prop;

  @Inject
  public ValidatorUtils(final Props prop) {
    this.prop = prop;
    this.validatorManager = new XmlValidatorManager(this.prop);
  }

  public Map<String, ValidationReport> validateProject(final Project project, final File folder) {
    // Reload XmlValidatorManager and create new object for each project verification because props
    // must be unique between validations.
    final ValidatorManager validatorManager = new XmlValidatorManager(this.prop);
    logger.info("Validating project " + project.getName()
        + " using the registered validators "
        + validatorManager.getValidatorsInfo().toString());
    return validatorManager.validate(project, folder);
  }
}
