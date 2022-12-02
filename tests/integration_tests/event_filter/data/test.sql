DROP DATABASE IF EXISTS `event_filter`;

CREATE DATABASE `event_filter`;

USE `event_filter`;

/* specify a event filter matcher to this table t1 */
CREATE TABLE t1 (
  id INT,
  name varchar(128),
  country char(32),
  city varchar(64),
  age INT,
  gender char(32),
  PRIMARY KEY (id)
);

/* do not ignore*/
INSERT INTO t1
VALUES (1, 'guagua', "china", "chengdu", 1, "female");

/* ignore by id*/
INSERT INTO t1
VALUES (2, 'huahua', "china", "chengdu", 2, "female");

/* ignore by city*/
INSERT INTO t1
VALUES (3, 'xigua', "japan", "tokyo", 2, "male");

/* do not ignore*/
INSERT INTO t1
VALUES (4, 'yuko', "japan", "nagoya", 33, "female");

/* ignore by event type*/
DELETE FROM t1
WHERE id = 4;

/* ignore by event type*/
DROP TABLE t1;

/* all event of t2 will be replicated to downstream */
CREATE TABLE t2 (
                    id INT,
                    name varchar(128),
                    country char(32),
                    city varchar(64),
                    age INT,
                    gender char(32),
                    PRIMARY KEY (id)
);
INSERT INTO t2
VALUES (1, 'guagua', "china", "chengdu", 1, "female");

INSERT INTO t2
VALUES (2, 'huahua', "china", "chengdu", 2, "female");

INSERT INTO t2
VALUES (3, 'xigua', "japan", "tokyo", 2, "male");

INSERT INTO t2
VALUES (4, 'yuko', "japan", "nagoya", 33, "female");

create table finish_mark(id int primary key);
