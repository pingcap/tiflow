USE `event_filter`;

INSERT INTO t_name
VALUES (1, 'guagua');

INSERT INTO t_name
VALUES (2, 'huahua');

RENAME TABLE t_name MODIFY t_rename BIGINT;

INSERT INTO t_rename
VALUES (3, 'xigua');

INSERT INTO t_rename
VALUES (4, 'yuko');
