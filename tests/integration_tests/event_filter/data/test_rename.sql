USE `event_filter`;

RENAME TABLE t_name TO t_rename;

INSERT INTO t_rename
VALUES (1, 'guagua');

INSERT INTO t_rename
VALUES (2, 'huahua');

INSERT INTO t_rename
VALUES (3, 'xigua');

INSERT INTO t_rename
VALUES (4, 'yuko');

-- rename tables
RENAME TABLE t_name1 TO t_rename1, t_name2 TO t_rename2, t_name3 TO t_rename3;

INSERT INTO t_rename1
VALUES (1, 'guagua');

INSERT INTO t_rename2
VALUES (2, 'huahua');

INSERT INTO t_rename3
VALUES (3, 'xigua');
