DROP DATABASE IF EXISTS mariadb_source;
-- Use utf8mb4_general_ci explicitly: MariaDB 11.4+ defaults to
-- utf8mb4_uca1400_ai_ci which TiDB does not support yet.
CREATE DATABASE mariadb_source DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE mariadb_source;

CREATE TABLE t1 (
  id INT PRIMARY KEY,
  name VARCHAR(32) NOT NULL
);

INSERT INTO t1 (id, name) VALUES
  (1, 'alpha'),
  (2, 'beta');
