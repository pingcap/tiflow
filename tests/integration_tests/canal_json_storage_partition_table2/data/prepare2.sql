use `partition_table`;

-- partition p0
insert into t1 values (4),(5);
-- partition p1
insert into t1 values (10);
-- partition p2
insert into t1 values (13),(14),(15),(16),(17);

create table finish_mark (a int primary key);