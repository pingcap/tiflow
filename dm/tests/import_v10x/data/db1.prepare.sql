create database if not exists `import_v10x`;
use `import_v10x`;
create table t1 (id int NOT NULL AUTO_INCREMENT, name varchar(20), PRIMARY KEY (id));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
