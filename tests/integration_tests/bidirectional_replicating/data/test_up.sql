use `bidirectional_replication`;

insert into a1 values (2);
insert into a1 values (3);
insert into a1 values (4);
insert into a1 values (5);
insert into a1 values (6);

create table finish_mark (id int primary key);
