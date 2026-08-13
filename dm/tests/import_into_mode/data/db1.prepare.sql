drop database if exists `import_into_mode`;
create database `import_into_mode`;
use `import_into_mode`;

-- basic table with primary key
create table t1 (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(20),
    PRIMARY KEY (id)
);
insert into t1 (id, name) values (1, 'alice'), (2, 'bob');
insert into t1 (id, name) values (3, 'charlie'), (4, 'david');

-- table with various column types
create table t2 (
    id int PRIMARY KEY,
    name varchar(50),
    age int,
    salary decimal(10, 2),
    created_at datetime DEFAULT CURRENT_TIMESTAMP
);
insert into t2 (id, name, age, salary) values (1, 'user1', 25, 5000.50);
insert into t2 (id, name, age, salary) values (2, 'user2', 30, 6000.75);
insert into t2 (id, name, age, salary) values (3, 'user3', 35, 7500.00);
