use Upper_DB;
insert into Do_Table (id, name, dt, ts) values (100, 'Eddard Stark', now(), '2021-05-11 12:01:05');
alter table Do_Table drop column `dt`;
insert into Do_Table (id, name, ts) values (101, 'Spike', '2021-05-31 12:01:05');
alter table Do_Table add index `idx_name_ts` (`name`, `ts`);
insert into Do_Table (id, name, ts) values (102, 'Jet', '2021-05-31 13:01:00');

use Upper_DB1;
insert into Do_Table1 (id, name) values (3, 'third');
alter table Do_Table1 add column info json;
alter table Do_Table1 add column gen_id int as (info->'$.id');
alter table Do_Table1 add index multi_col(`name`, `gen_id`);
insert into Do_Table1 (id, name, info) values (4, 'gentest', '{"id": 123}');
insert into Do_Table1 (id, name, info) values (5, 'gentest', '{"id": 124}');
update Do_Table1 set info = '{"id": 120}' where id = 1;
update Do_Table1 set info = '{"id": 121}' where id = 2;
update Do_Table1 set info = '{"id": 122}' where id = 3;

use `Upper_Db_IGNORE`;
insert into ignore_table (id) values (0);
alter table ignore_table add column description varchar(32);
insert into ignore_table (id, description) values (0, 'ignored');
