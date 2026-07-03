drop database if exists test;
create database test;
use test;

create table tp_int
(
    id          int auto_increment,
    c_tinyint   tinyint   null,
    c_smallint  smallint  null,
    c_mediumint mediumint null,
    c_int       int       null,
    c_bigint    bigint    null,
    constraint pk
        primary key (id)
);


-- unsigned int
create table tp_unsigned_int (
    id          int auto_increment,
    c_unsigned_tinyint   tinyint   unsigned null,
    c_unsigned_smallint  smallint  unsigned null,
    c_unsigned_mediumint mediumint unsigned null,
    c_unsigned_int       int       unsigned null,
    c_unsigned_bigint    bigint    unsigned null,
    constraint pk
        primary key (id)
);


-- real
create table tp_real
(
    id        int auto_increment,
    c_float   float   null,
    c_double  double  null,
    c_decimal decimal null,
    c_decimal_2 decimal(10, 4) null,
    constraint pk
        primary key (id)
);



-- unsigned real
create table tp_unsigned_real (
    id                   int auto_increment,
    c_unsigned_float     float unsigned   null,
    c_unsigned_double    double unsigned  null,
    c_unsigned_decimal   decimal unsigned null,
    c_unsigned_decimal_2 decimal(10, 4) unsigned null,
    constraint pk
        primary key (id)
);

create table finish_mark_for_ddl
(
    id int PRIMARY KEY
);


