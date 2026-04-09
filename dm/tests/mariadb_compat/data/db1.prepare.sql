DROP DATABASE IF EXISTS mariadbcompat;
CREATE DATABASE mariadbcompat DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE mariadbcompat;

CREATE TABLE t_full (
  id INT PRIMARY KEY,
  txt TEXT DEFAULT 'x',
  v VARCHAR(800),
  j JSON,
  CHECK (json_valid(j)),
  KEY idx_txt (txt),
  KEY idx_v (v)
) DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

INSERT INTO t_full (id, txt, v, j) VALUES
  (1, 'alpha', 'short', '{"a":1}'),
  (2, 'beta', RPAD('v', 700, 'v'), '{"a":2}');
