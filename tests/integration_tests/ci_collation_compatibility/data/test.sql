drop database if exists `ci_collation_compatibility`;
create database `ci_collation_compatibility`;
use `ci_collation_compatibility`;

CREATE TABLE t1 (
    a varchar(20) not null,
    b int default 10,
    PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */
);

insert into t1 values ('hello', 1);
