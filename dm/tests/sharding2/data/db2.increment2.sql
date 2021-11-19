alter table sharding22.t1 add column name varchar(20);
alter table sharding22.t1 add index abc(name);

insert into sharding22.t1 values(10, "d"), (11, "e");

alter table sharding22.t1 add column c int;
alter table sharding22.t1 add index c(c);
alter table sharding22.t1 add column d int;
alter table sharding22.t1 add index d(d);
alter table sharding22.t1 add column e int, add index e(e);

insert into sharding22.t1 values(12, "g", 12, 12, 12);
insert into sharding22.t1 values(13, "h", 13, 13, 13);

alter table sharding22.t1 add column f int;
