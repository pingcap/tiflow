drop database if exists `ci_collation_compatibility`;
create database `ci_collation_compatibility`;
use `ci_collation_compatibility`;

CREATE TABLE t (
    a varchar(20) not null PRIMARY KEY CLUSTERED,
    b int default 10
);

insert into t values ('hello', 1);
