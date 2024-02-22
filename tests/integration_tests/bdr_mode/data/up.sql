drop database if exists `bdr_mode`;
admin set bdr role primary;
create database `bdr_mode`;
use `bdr_mode`;

create table `t1` (id int primary key, name varchar(20));
create table `t2` (id int primary key, name varchar(20));

begin;
insert into `t1` values (1, '1'), (3, '3'), (5, '5'), (7, '7'), (9, '9');
commit;

begin;
update `t1` set `name` = '11' where `id` = 1;
delete from `t1` where `id` = 3;
update `t1` set `name` = '55' where `id` = 5;
delete from `t1` where `id` = 7;
commit;

begin;
insert into `t1` values (22, '22'), (44, '44'), (66, '66'), (88, '88'), (108, '108');
rollback;

insert into `t1` values (100, '100'), (300, '300'), (500, '500'), (700, '700'), (900, '900');

insert into `t2` values (1, '1'), (3, '3'), (5, '5'), (7, '7'), (9, '9');
insert into `t2` values (2, '2'), (4, '4'), (6, '6'), (8, '8'), (10, '10');


/* Test ddl operation */

create table t3(
    id int primary key,
    name varchar(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
)
PARTITION BY RANGE (id)
(
    PARTITION p0 VALUES LESS THAN (100),
    PARTITION p1 VALUES LESS THAN (200),
    PARTITION p2 VALUES LESS THAN (300),
    PARTITION p3 VALUES LESS THAN (400)
);

create table t4(
    id int primary key,
    name varchar(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY `idx_name_id` (`name`,`id`)
)
PARTITION BY RANGE (id)
(
    PARTITION p0 VALUES LESS THAN (100),
    PARTITION p1 VALUES LESS THAN (200),
    PARTITION p2 VALUES LESS THAN (300),
    PARTITION p3 VALUES LESS THAN (400)
);

INSERT INTO t3 (id, name) VALUES (1, 'John');
INSERT INTO t3 (id, name) VALUES (2, 'Emma');
INSERT INTO t3 (id, name) VALUES (3, 'Michael');
INSERT INTO t3 (id, name) VALUES (4, 'Sophia');
INSERT INTO t3 (id, name) VALUES (5, 'Daniel');


INSERT INTO t4 (id, name) VALUES (1, 'John');
INSERT INTO t4 (id, name) VALUES (2, 'Emma');
INSERT INTO t4 (id, name) VALUES (3, 'Michael');
INSERT INTO t4 (id, name) VALUES (4, 'Sophia');
INSERT INTO t4 (id, name) VALUES (5, 'Daniel');

-- create view
CREATE VIEW tv3 AS
SELECT name
FROM t3
WHERE name != "John";

-- create and drop view
CREATE VIEW tv4 AS
SELECT name
FROM t4
WHERE name != "Emma";

DROP VIEW tv4;

-- insert some data 
INSERT INTO t3 (id, name) VALUES (6, 'John 2');
INSERT INTO t3 (id, name) VALUES (7, 'Emma 2');
INSERT INTO t4 (id, name) VALUES (8, 'Michael 2');
INSERT INTO t4 (id, name) VALUES (9, 'Sophia 2');

-- -- alter table add ttl 
-- ALTER TABLE t3 TTL = `created_at` + INTERVAL 1 MONTH;
-- ALTER TABLE t3 TTL_ENABLE = 'OFF';


-- -- alter table add and remove ttl 
-- ALTER TABLE t4 TTL = `created_at` + INTERVAL 1 MONTH;
-- ALTER TABLE t4 REMOVE TTL;


-- alter table comment
ALTER TABLE t3 COMMENT = 'test comment';
ALTER TABLE t3 COMMENT = 'test comment 2';

-- insert some data
INSERT INTO t3 (id, name) VALUES (10, 'Daniel 2');
INSERT INTO t3 (id, name) VALUES (11, 'John 3');
INSERT INTO t4 (id, name) VALUES (13, 'Michael 3');
INSERT INTO t4 (id, name) VALUES (14, 'Sophia 3');

-- add partition 
ALTER TABLE t3 ADD PARTITION (PARTITION p4 VALUES LESS THAN (500));


-- insert some data 
INSERT INTO t3 (id, name) VALUES (15, 'Daniel 3');
INSERT INTO t3 (id, name) VALUES (16, 'John 4');
INSERT INTO t4 (id, name) VALUES (17, 'Michael 4');
INSERT INTO t4 (id, name) VALUES (18, 'Sophia 4');

-- ALTER TABLE ALTER COLUME DEFAULT value
ALTER TABLE t3 ALTER COLUMN name SET DEFAULT 'test';

-- insert some data
INSERT INTO t3 (id, name) VALUES (19, 'Daniel 5');
INSERT INTO t3 (id, name) VALUES (20, 'John 5');
INSERT INTO t4 (id, name) VALUES (21, 'Michael 5');
INSERT INTO t4 (id, name) VALUES (22, 'Sophia 5');


-- add column that can be null
ALTER TABLE t3 ADD COLUMN age int;
-- add not null column with default value
ALTER TABLE t3 ADD COLUMN gender varchar(255) not null default 'male';

-- insert some data
INSERT INTO t3 (id, name) VALUES (23, 'Daniel 6');
INSERT INTO t3 (id, name) VALUES (24, 'John 6');
INSERT INTO t4 (id, name) VALUES (25, 'Michael 6');
INSERT INTO t4 (id, name) VALUES (26, 'Sophia 6');

-- add not unique index
ALTER TABLE t3 ADD INDEX idx_name (name);
ALTER TABLE t3 ADD INDEX idx_name_age (name, age);

-- insert some data
INSERT INTO t3 (id, name) VALUES (27, 'Daniel 7');
INSERT INTO t3 (id, name) VALUES (28, 'John 7');
INSERT INTO t4 (id, name) VALUES (29, 'Michael 7');
INSERT INTO t4 (id, name) VALUES (30, 'Sophia 7');


DO SLEEP(30); -- waiting for `add index` finish in downstream TiDB
-- drop not unique index
ALTER TABLE t3 DROP INDEX idx_name_age;

-- insert some data
INSERT INTO t3 (id, name) VALUES (31, 'Daniel 8');
INSERT INTO t3 (id, name) VALUES (32, 'John 8');
INSERT INTO t4 (id, name) VALUES (33, 'Michael 8');
INSERT INTO t4 (id, name) VALUES (34, 'Sophia 8');

-- alter index name 
ALTER TABLE t3 RENAME INDEX idx_name TO idx_name_new;

-- alter index visibility
ALTER TABLE t3 ALTER INDEX idx_name_new INVISIBLE;
ALTER TABLE t3 ALTER INDEX idx_name_new VISIBLE;

-- insert some data
INSERT INTO t3 (id, name) VALUES (35, 'Daniel 9');
INSERT INTO t3 (id, name) VALUES (36, 'John 9');
INSERT INTO t4 (id, name) VALUES (37, 'Michael 9');
INSERT INTO t4 (id, name) VALUES (38, 'Sophia 9');

-- drop UK 
ALTER TABLE t4 DROP INDEX idx_name_id;


-- insert some data
INSERT INTO t3 (id, name) VALUES (39, 'Daniel 10');
INSERT INTO t3 (id, name) VALUES (40, 'John 10');
INSERT INTO t4 (id, name) VALUES (41, 'Michael 10');
INSERT INTO t4 (id, name) VALUES (42, 'Sophia 10');