USE mariadb_source;

ALTER TABLE t1
  ADD COLUMN note VARCHAR(32) DEFAULT '';

UPDATE t1
SET name = 'beta_updated', note = 'updated'
WHERE id = 2;

INSERT INTO t1 (id, name, note) VALUES
  (3, 'gamma', 'inserted');
