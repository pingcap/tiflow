use `new_collation_off`;

insert into t1 (id, name) values (2, 'Bob');

create table t2 (
    id int PRIMARY KEY,
    name varchar(20) COLLATE utf8mb4_0900_as_cs
);

insert into t2 (id, name) values (3, 'Charlie');
