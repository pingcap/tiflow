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

/* all event of t_normal will be replicated to downstream */
CREATE TABLE t_normal (
                    id INT,
                    name varchar(128),
                    country char(32),
                    city varchar(64),
                    age INT,
                    gender char(32),
                    PRIMARY KEY (id)
);
INSERT INTO t_normal
VALUES (1, 'guagua', "china", "chengdu", 1, "female");

INSERT INTO t_normal
VALUES (2, 'huahua', "china", "chengdu", 2, "female");

INSERT INTO t_normal
VALUES (3, 'xigua', "japan", "tokyo", 2, "male");

INSERT INTO t_normal
VALUES (4, 'yuko', "japan", "nagoya", 33, "female");

CREATE TABLE t_truncate (
                    id INT,
                    name varchar(128),
                    PRIMARY KEY (id)
);
CREATE TABLE t_alter
(
    id          INT AUTO_INCREMENT,
    t_boolean   BOOLEAN,
    t_bigint    DECIMAL(38, 19),
    t_double    DOUBLE,
    t_decimal   DECIMAL(38, 19),
    t_bit       BIT(64),
    t_date      DATE,
    t_datetime  DATETIME,
    t_timestamp TIMESTAMP NULL,
    t_time      TIME,
    t_year      YEAR,
    t_char      CHAR,
    t_varchar   VARCHAR(10),
    t_blob      BLOB,
    t_text      TEXT,
    t_enum      ENUM ('enum1', 'enum2', 'enum3'),
    t_set       SET ('a', 'b', 'c'),
    t_json      JSON,
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COLLATE = utf8_bin;
  

CREATE TABLE t_name (
                    id INT,
                    name varchar(128),
                    PRIMARY KEY (id)
);
CREATE TABLE t_name1 (
                    id INT,
                    name varchar(128),
                    PRIMARY KEY (id)
);
CREATE TABLE t_name2 (
                    id INT,
                    name varchar(128),
                    PRIMARY KEY (id)
);
CREATE TABLE t_name3 (
                    id INT,
                    name varchar(128),
                    PRIMARY KEY (id)
);