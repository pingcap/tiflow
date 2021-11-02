alter table sharding22.t1 add column name varchar(20);
alter table sharding22.t1 add index abc(name);

insert into sharding22.t1 values(1, "a"), (2, "b");
insert into sharding22.t1 values(3, "c"), (4, "d");
insert into sharding22.t1 values(5, "e"), (6, "f");

alter table sharding22.t1 add column c int;
alter table sharding22.t1 add index c(c);
alter table sharding22.t1 add column d int;
alter table sharding22.t1 add index d(d);
alter table sharding22.t1 add column e int, add index e(e);

insert into sharding22.t1 values(7, "g", 7, 7, 7);
insert into sharding22.t1 values(8, "h", 8, 8, 8);
insert into sharding22.t1 values(9, "i", 9, 9, 9);

alter table sharding22.t1 add column f int;
