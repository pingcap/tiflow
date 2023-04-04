use `ddl_only_block_related_table`;

-- this will block `ddl_not_done` table from being replicated
alter table ddl_not_done add column `good_id` bigint(20) unsigned null;

insert into t1 values (2);
insert into t2 values (2);

