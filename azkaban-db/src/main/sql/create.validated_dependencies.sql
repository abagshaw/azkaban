CREATE TABLE validated_dependencies (
  file_name         VARCHAR(100),
  file_sha1         VARCHAR(40),
  validation_key    VARCHAR(40),
  validation_status INT,
  PRIMARY KEY (file_sha1, file_name, validation_key)
);
