use test;
insert into table10(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values(6,7,8,9,10);

insert into table20(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values(6,7,8,9,10);

drop table table20, table3;

create table finish
(
    id int PRIMARY KEY
);