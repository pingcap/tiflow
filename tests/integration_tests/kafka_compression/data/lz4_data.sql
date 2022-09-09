use test;

create table tp_int_lz4
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

insert into tp_int_lz4(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);

create table lz4_finish_mark
(
    id int PRIMARY KEY
);
