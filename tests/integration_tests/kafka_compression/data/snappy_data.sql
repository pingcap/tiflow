use test;

create table tp_int_snappy
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

insert into tp_int_snappy(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);

create table snappy_finish_mark
(
    id int PRIMARY KEY
);
