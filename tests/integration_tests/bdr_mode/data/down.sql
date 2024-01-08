admin set bdr role secondary;

use `bdr_mode`;

begin;
insert into `t1` values (2, '2'), (4, '4'), (6, '6'), (8, '8'), (10, '10');
commit;

begin;
update `t1` set `name` = '22' where `id` = 2;
delete from `t1` where `id` = 4;
update `t1` set `name` = '66' where `id` = 6;
delete from `t1` where `id` = 8;
commit;

begin;
insert into `t1` values (19, '19'), (39, '39'), (59, '59'), (79, '79'), (99, '99');
rollback;

insert into `t1` values (200, '200'), (400, '400'), (600, '600'), (800, '800'), (1000, '1000');

