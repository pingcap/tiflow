alter table new_relay.t1 add column new_col1 int, add column new_col2 int;
insert into new_relay.t1 (id, name, info, new_col1, new_col2) values (13, 'gentest', '{"id": 133}', 13, 13), (14, 'gentest', '{"id": 134}', 14, 14);
