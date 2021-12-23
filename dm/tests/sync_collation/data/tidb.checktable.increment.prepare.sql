use `sync_collation_increment`;
create table t_check (id int, name varchar(20), primary key (`id`));
insert into t_check (id, name) values (1, 'Aa'), (2, 'aA');
use `sync_collation_increment2`;
create table t_check (id int, name varchar(20), primary key (`id`));
insert into t_check (id, name) values (1, 'Aa'), (2, 'aA');