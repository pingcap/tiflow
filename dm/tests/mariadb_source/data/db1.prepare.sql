DROP DATABASE IF EXISTS mariadb_source;
CREATE DATABASE mariadb_source;
USE mariadb_source;

CREATE TABLE t1 (
  id INT PRIMARY KEY,
  name VARCHAR(32) NOT NULL
);

INSERT INTO t1 (id, name) VALUES
  (1, 'alpha'),
  (2, 'beta');
