use test;

create table table1
(
    id          int auto_increment,
    c_tinyint   tinyint null,
    c_smallint  smallint null,
    c_mediumint mediumint null,
    c_int       int null,
    c_bigint    bigint null,
    constraint pk
        primary key (id)
);
insert into table1(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);

create table table2
(
    id          int auto_increment,
    c_tinyint   tinyint null,
    c_smallint  smallint null,
    c_mediumint mediumint null,
    c_int       int null,
    c_bigint    bigint null,
    constraint pk
        primary key (id)
);
insert into table2(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);

create table table3
(
    id          int auto_increment,
    c_tinyint   tinyint null,
    c_smallint  smallint null,
    c_mediumint mediumint null,
    c_int       int null,
    c_bigint    bigint null,
    constraint pk
        primary key (id)
);

insert into table3(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);