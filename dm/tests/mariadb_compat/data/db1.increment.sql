USE mariadbcompat;

CREATE OR REPLACE TABLE t_replace (
  id INT PRIMARY KEY,
  note TEXT DEFAULT 'x',
  KEY idx_note (note)
) DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

INSERT INTO t_replace (id, note) VALUES
  (1, 'first'),
  (2, 'second');

ALTER TABLE t_replace
  ADD COLUMN late_txt TEXT DEFAULT 'y';
