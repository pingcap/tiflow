drop database if exists `clustered_index_test`;
create database `clustered_index_test`;
use `clustered_index_test`;

set @@tidb_enable_clustered_index=1;

CREATE TABLE t0 (
	id VARCHAR(255),
	data INT,
	PRIMARY KEY(id)
);

INSERT INTO t0 VALUES ('1', 1);
INSERT INTO t0 VALUES ('2', 2);
INSERT INTO t0 VALUES ('3', 3);
INSERT INTO t0 VALUES ('4', 4);
INSERT INTO t0 VALUES ('5', 5);

DELETE FROM t0 WHERE id = '3';
DELETE FROM t0 WHERE data = 5;
UPDATE t0 SET id = '10' WHERE data = 1;
UPDATE t0 SET data = 555 WHERE id = '10';

CREATE TABLE t1 (
    id VARCHAR(255),
    a INT,
    b CHAR(10),
    PRIMARY KEY(id, b),
    UNIQUE KEY(b),
    KEY(a)
);

INSERT INTO t1 VALUES ('111', 111, '111');
INSERT INTO t1 VALUES ('222', 222, '222');
INSERT INTO t1 VALUES ('333', 333, '333');
INSERT INTO t1 VALUES ('444', 444, '444');
INSERT INTO t1 VALUES ('555', 555, '555');
UPDATE t1 SET id = '10' WHERE id = '111';
DELETE FROM t1 WHERE a = 222;

CREATE TABLE t2 (
    id VARCHAR(255),
    a INT,
    b DECIMAL(5,2),
    PRIMARY KEY(id, a),
    KEY(id, a),
    UNIQUE KEY(id, a)
);

INSERT INTO t2 VALUES ('aaaa', 1111, 11.0);
INSERT INTO t2 VALUES ('bbbb', 1111, 12.0);
INSERT INTO t2 VALUES ('cccc', 1111, 13.0);
INSERT INTO t2 VALUES ('dddd', 1111, 14.0);
INSERT INTO t2 VALUES ('eeee', 1111, 15.0);
