drop database if exists `clustered_index_test`;
create database `clustered_index_test`;
use `clustered_index_test`;

set @@tidb_enable_clustered_index=1;

CREATE TABLE simple (
	id VARCHAR(255),
	data INT,
	PRIMARY KEY(id)
);

INSERT INTO simple VALUES ('1', 1);
INSERT INTO simple VALUES ('2', 2);
INSERT INTO simple VALUES ('3', 3);
INSERT INTO simple VALUES ('4', 4);
INSERT INTO simple VALUES ('5', 5);

DELETE FROM simple WHERE id = '3';
DELETE FROM simple WHERE data = 5;
UPDATE simple SET id = '10' WHERE data = 1;
UPDATE simple SET data = 555 WHERE id = '10';

CREATE TABLE multi_index (
    id VARCHAR(255),
    a INT,
    b CHAR(10),
    PRIMARY KEY(id, b),
    UNIQUE KEY(b),
    KEY(a)
)

INSERT INTO multi_index VALUES ('111', 111, '111');
INSERT INTO multi_index VALUES ('222', 222, '222');
INSERT INTO multi_index VALUES ('333', 333, '333');
INSERT INTO multi_index VALUES ('444', 444, '444');
INSERT INTO multi_index VALUES ('555', 555, '555');
UPDATE multi_index SET id = '10' WHERE data = 1;
DELETE FROM multi_index WHERE a = 222;

CREATE TABLE index_on_same_col (
    id VARCHAR(255),
    a INT,
    b DECIMAL(5,2),
    PRIMARY(id, a),
    KEY(id, a),
    UNIQUE KEY(id, a)
)

INSERT INTO index_on_same_col VALUES ('aaaa', 1111, 11.0);
INSERT INTO index_on_same_col VALUES ('bbbb', 1111, 12.0);
INSERT INTO index_on_same_col VALUES ('cccc', 1111, 13.0);
INSERT INTO index_on_same_col VALUES ('dddd', 1111, 14.0);
INSERT INTO index_on_same_col VALUES ('eeee', 1111, 15.0);
