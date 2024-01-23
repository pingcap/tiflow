USE `event_filter`;

/* test new physical table is replicated */
TRUNCATE TABLE t_truncate;
INSERT INTO t_truncate
VALUES (1, 'guagua');

INSERT INTO t_truncate
VALUES (2, 'huahua');

INSERT INTO t_truncate
VALUES (3, 'xigua');

INSERT INTO t_truncate
VALUES (4, 'yuko');
