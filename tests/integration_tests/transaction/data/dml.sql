use test;
BEGIN;
create table start_mark
(
    id int PRIMARY KEY
);
create table start_mark1
(
    id int PRIMARY KEY
);
create table start_mark2
(
    id int PRIMARY KEY
);
create table start_mark3
(
    id int PRIMARY KEY
);
INSERT INTO test.t1 VALUES (1, 1);
create table finish_mark
(
    id int PRIMARY KEY
);
COMMIT;
