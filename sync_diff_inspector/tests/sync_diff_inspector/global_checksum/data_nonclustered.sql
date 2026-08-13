DROP DATABASE IF EXISTS checksum_mode_nonclustered;
CREATE DATABASE checksum_mode_nonclustered;
USE checksum_mode_nonclustered;

CREATE TABLE t (
  id BIGINT NOT NULL,
  k  BIGINT NOT NULL,
  v  VARCHAR(32),
  PRIMARY KEY (id) NONCLUSTERED,
  KEY idx_k (k)
);

INSERT INTO t VALUES
  (1, 101, 'a'),
  (2, 102, 'b'),
  (3, 103, 'c'),
  (4, 104, 'd'),
  (5, 105, 'e');
