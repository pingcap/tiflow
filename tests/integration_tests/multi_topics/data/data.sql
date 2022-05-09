use test;

create table test1
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

insert into test1(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);

create table test2
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

insert into test2(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);

create table test3
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

insert into test3(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);
