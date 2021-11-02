alter table new_relay.t1 drop column notused, drop column notused2;
insert into new_relay.t1 (id, name, info) values (11, 'gentest', '{"id": 131}'), (12, 'gentest', '{"id": 132}');
