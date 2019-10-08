package azkaban.project.validator;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * The result of a project validation generated by a {@link ProjectValidator}. It contains an enum
 * of type {@link ValidationStatus} representing whether the validation passes, generates warnings,
 * or generates errors. Accordingly, three sets of String are also maintained, storing the messages
 * generated by the {@link ProjectValidator} at both {@link ValidationStatus#WARN} and {@link
 * ValidationStatus#ERROR} level, as well as information messages associated with both levels.
 */
public class ValidationReport {

  protected ValidationStatus _status;
  protected Set<File> _removedFiles;
  protected Set<File> _modifiedFiles;
  protected Set<String> _infoMsgs;
  protected Set<String> _warningMsgs;
  protected Set<String> _errorMsgs;

  public ValidationReport() {
    this._status = ValidationStatus.PASS;
    this._removedFiles = new HashSet<>();
    this._modifiedFiles = new HashSet<>();
    this._infoMsgs = new HashSet<>();
    this._warningMsgs = new HashSet<>();
    this._errorMsgs = new HashSet<>();
  }

  /**
   * Return the severity level this information message is associated with.
   */
  public static ValidationStatus getInfoMsgLevel(final String msg) {
    if (msg.startsWith("ERROR")) {
      return ValidationStatus.ERROR;
    }
    if (msg.startsWith("WARN")) {
      return ValidationStatus.WARN;
    }
    return ValidationStatus.PASS;
  }

  /**
   * Get the raw information message.
   */
  public static String getInfoMsg(final String msg) {
    if (msg.startsWith("ERROR")) {
      return msg.replaceFirst("ERROR", "");
    }
    if (msg.startsWith("WARN")) {
      return msg.replaceFirst("WARN", "");
    }
    return msg;
  }

  /**
   * Add an information message associated with warning messages
   */
  public void addWarnLevelInfoMsg(final String msg) {
    if (msg != null) {
      this._infoMsgs.add("WARN" + msg);
    }
  }

  /**
   * Add an information message associated with error messages
   */
  public void addErrorLevelInfoMsg(final String msg) {
    if (msg != null) {
      this._infoMsgs.add("ERROR" + msg);
    }
  }

  /**
   * Add a message with status level being {@link ValidationStatus#WARN}
   */
  public void addWarningMsgs(final Set<String> msgs) {
    if (msgs != null) {
      this._warningMsgs.addAll(msgs);
      if (!msgs.isEmpty() && this._errorMsgs.isEmpty()) {
        this._status = ValidationStatus.WARN;
      }
    }
  }

  /**
   * Add a message with status level being {@link ValidationStatus#ERROR}
   */
  public void addErrorMsgs(final Set<String> msgs) {
    if (msgs != null) {
      this._errorMsgs.addAll(msgs);
      if (!msgs.isEmpty()) {
        this._status = ValidationStatus.ERROR;
      }
    }
  }

  /**
   * Add a set of modified files
   */
  public void addModifiedFiles(final Set<File> files) {
    this._modifiedFiles.addAll(files);
  }

  /**
   * Add a set of removed files
   */
  public void addRemovedFiles(final Set<File> files) {
    this._removedFiles.addAll(files);
  }

  /**
   * Retrieve the status of the report.
   */
  public ValidationStatus getStatus() {
    return this._status;
  }

  /**
   * Retrieve the list of information messages.
   */
  public Set<String> getInfoMsgs() {
    return this._infoMsgs;
  }

  /**
   * Retrieve the messages associated with status level {@link ValidationStatus#WARN}
   */
  public Set<String> getWarningMsgs() {
    return this._warningMsgs;
  }

  /**
   * Retrieve the messages associated with status level {@link ValidationStatus#ERROR}
   */
  public Set<String> getErrorMsgs() {
    return this._errorMsgs;
  }

  /**
   * Get the set of modified files
   */
  public Set<File> getModifiedFiles() { return this._modifiedFiles; }

  /**
   * Get the set of removed files
   */
  public Set<File> getRemovedFiles() { return this._removedFiles; }
}
