drop database if exists `autorandom_test`;
create database `autorandom_test`;
use `autorandom_test`;

CREATE TABLE table_a (
	id BIGINT AUTO_RANDOM,
	data int,
	PRIMARY KEY(id) clustered
);

-- test for `nullable`
INSERT INTO table_a() VALUES ();

INSERT INTO table_a (data) value (1);
INSERT INTO table_a (data) value (2);
INSERT INTO table_a (data) value (3);
INSERT INTO table_a (data) value (4);
INSERT INTO table_a (data) value (5);
