flush logs;
insert into new_relay.t1 (id, name, info) values (8, 'gentest', '{"id": 128}');
insert into new_relay.t1 (id, name, info) values (9, 'gentest', '{"id": 129}'), (10, 'gentest', '{"id": 130}');
alter table new_relay.t1 add column notused int;
