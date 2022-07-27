drop database if exists `sequence_test`;
create database `sequence_test`;
use `sequence_test`;

CREATE SEQUENCE seq0 start with 1 minvalue 1 maxvalue 999999999999999 increment by 1 nocache cycle;

-- select seq0
SELECT next value for seq0;
-- select again
SELECT next value for seq0;

-- t1 refers seq0
-- note that only TiDB supports it.
CREATE TABLE t1 (
    id VARCHAR(255),
    a INT default next value for seq0,
    PRIMARY KEY(id)
);

-- TiCDC is able to replicate following changes to TiDB.
INSERT INTO t1 (id) VALUES ('111');
INSERT INTO t1 (id) VALUES ('222');
INSERT INTO t1 (id) VALUES ('333');
UPDATE t1 SET id = '10' WHERE id = '111';
DELETE FROM t1 WHERE a = 222;
