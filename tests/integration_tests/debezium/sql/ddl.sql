/* CREATE TABLE */
CREATE TABLE t1 (
  PK INT PRIMARY KEY,
  COL INT
);

/* CREATE DATABASE */
CREATE DATABASE foo;
USE foo;

CREATE TABLE bar (
  PK INT PRIMARY KEY AUTO_INCREMENT,
  COL INT
);
INSERT INTO bar VALUES (1, 1);

/* VIEW */
CREATE VIEW V1 AS 
SELECT *
FROM bar
WHERE COL > 2;
DROP VIEW IF EXISTS V1;

/* ALTER COLUMN */
ALTER TABLE bar
ADD COLUMN COL2 INT;
ALTER TABLE bar
MODIFY COLUMN COL2 FLOAT;
ALTER TABLE bar
DROP COLUMN COL2;

/* Rebase AutoID */
ALTER TABLE bar AUTO_INCREMENT=310;

/* Set DEFAULT value */
ALTER TABLE bar
ALTER COL SET DEFAULT 3;

/* Modify TABLE comment */
ALTER TABLE bar COMMENT = 'New table comment';

/* Modify TABLE charset */
ALTER TABLE bar CHARACTER SET = utf8mb4 COLLATE utf8mb4_unicode_ci;

/* Modify DATABASE charset */
ALTER DATABASE foo CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE t1 (
  PK INT PRIMARY KEY,
  COL INT
);

/* MultiSchemaChange */
ALTER TABLE test.t1 
CHANGE COL COL2 VARCHAR(255);
ALTER TABLE foo.t1 
CHANGE COL COL2 VARCHAR(255);

/* PARTITION */
CREATE TABLE t2 (
    id INT NOT NULL,
    year_col INT NOT NULL
)
PARTITION BY RANGE (year_col) (
    PARTITION p0 VALUES LESS THAN (1991),
    PARTITION p1 VALUES LESS THAN (1995),
    PARTITION p2 VALUES LESS THAN (1999)
);
ALTER TABLE t2 ADD PARTITION (PARTITION p3 VALUES LESS THAN (2002));
ALTER TABLE t2 REORGANIZE PARTITION p3 INTO (
    PARTITION p31 VALUES LESS THAN (2002),
    PARTITION p32 VALUES LESS THAN (2005),
    PARTITION p33 VALUES LESS THAN (2008)
);
ALTER TABLE t2 REORGANIZE PARTITION p31,p32,p33,p2 INTO (PARTITION p21 VALUES LESS THAN (2008));
ALTER TABLE t2 TRUNCATE PARTITION p0;
ALTER TABLE t2 DROP PARTITION p0;
ALTER TABLE t2 PARTITION BY HASH(id) PARTITIONS 10;

/* ALTER INDEX visibility */
CREATE TABLE t3 (
  i INT,
  j INT,
  k INT,
  INDEX i_idx (i) INVISIBLE
) ENGINE = InnoDB;
CREATE INDEX j_idx ON t3 (j) INVISIBLE;
ALTER TABLE t3 ADD INDEX k_idx (k) INVISIBLE;
ALTER TABLE t3 ALTER INDEX i_idx VISIBLE;
ALTER TABLE t3 ALTER INDEX i_idx INVISIBLE;

/* INDEX */
CREATE TABLE t4 (col1 INT PRIMARY KEY, col2 INT);
CREATE INDEX idx1 ON t4 ((col1 + col2));
CREATE INDEX idx2 ON t4 ((col1 + col2), (col1 - col2), col1);
DROP INDEX idx1 ON t4;
ALTER TABLE t4 ADD INDEX ((col1 * 40) DESC);
ALTER TABLE t4 RENAME INDEX idx2 TO new_idx2;

/* PRIMARY KEY */
ALTER TABLE t4 DROP PRIMARY KEY pk(col1);
ALTER TABLE t4 ADD PRIMARY KEY pk(col1);

/*
  Adding a new column and setting it to the PRIMARY KEY is not supported.
  https://docs.pingcap.com/tidb/stable/sql-statement-add-column#mysql-compatibility
  ALTER TABLE t4 ADD COLUMN `id` INT(10) primary KEY;
*/
/* 
  Dropping primary key columns or columns covered by the composite index is not supported.
  https://docs.pingcap.com/tidb/stable/sql-statement-drop-column#mysql-compatibility
  ALTER TABLE t4 DROP PRIMARY KEY;
*/

/* EXCHANGE PARTITION */
CREATE TABLE t5 (
    id INT NOT NULL PRIMARY KEY,
    fname VARCHAR(30),
    lname VARCHAR(30)
)
    PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (50),
        PARTITION p1 VALUES LESS THAN (100),
        PARTITION p2 VALUES LESS THAN (150),
        PARTITION p3 VALUES LESS THAN (MAXVALUE)
);
INSERT INTO t5 VALUES (1669, "Jim", "Smith");
CREATE TABLE t6 LIKE t5;
ALTER TABLE t6 REMOVE PARTITIONING;
ALTER TABLE foo.t5 EXCHANGE PARTITION p0 WITH TABLE foo.t6;

CREATE TABLE t7 (
  PK INT PRIMARY KEY,
  COL INT
);
CREATE TABLE t8 (
  PK INT PRIMARY KEY,
  COL INT
);
CREATE TABLE t9 (
  PK INT PRIMARY KEY,
  COL INT
);

/* RENAME TABLE */
RENAME TABLE t7 TO rename_t7;
RENAME TABLE t8 To rename_t8, t9 To rename_t9;

/* TRUNCATE TABLE */
TRUNCATE TABLE rename_t7;

/* Debezium does not support */
/* RECOVER TABLE */
DROP TABLE t1;
RECOVER TABLE t1;

/* Debezium does not support */
/* TTL */
CREATE TABLE t10 (
    id int PRIMARY KEY,
    created_at TIMESTAMP
) /*T![ttl] TTL = `created_at` + INTERVAL 3 MONTH TTL_ENABLE = 'ON'*/;
ALTER TABLE t10 TTL = `created_at` + INTERVAL 1 MONTH;
ALTER TABLE t10 TTL_ENABLE = 'OFF';
ALTER TABLE t10 REMOVE TTL;

/* Debezium does not support */
/* VECTOR INDEX */
CREATE TABLE t11 (
    id       INT PRIMARY KEY,
    embedding     VECTOR(5)
);
CREATE VECTOR INDEX idx_embedding ON t11 ((VEC_COSINE_DISTANCE(embedding)));
ALTER TABLE t11 ADD VECTOR INDEX idx_embedding ((VEC_L2_DISTANCE(embedding))) USING HNSW;

/* DROP TABLE */
DROP TABLE foo.bar;
/* DROP DATABASE */
DROP DATABASE IF EXISTS foo;
