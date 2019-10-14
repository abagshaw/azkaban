CREATE TABLE validated_dependencies (
  file_sha1         VARCHAR(40),
  validation_key    VARCHAR(40),
  validation_status INT,
  PRIMARY KEY (file_sha1, validation_key)
);
