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
insert into `t1` values (1, '1'), (3, '3'), (5, '5'), (7, '7'), (9, '9');
rollback;
