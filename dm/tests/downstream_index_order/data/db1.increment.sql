use dm_syncer_tracker;
create table t(
  a int not null,
  b int not null,
  c int not null,
  d int not null,
  e int not null,
  index (a),
  index(b),
  index(c)
);

alter table t add unique(d);
alter table t add unique(e);
insert into t values(1,1,1,1,1);
