use online_ddl;
insert into check_gho_t (uid, name) values (10003, 'Buenos Aires');
update check_gho_t set name = 'Gabriel José de la Concordia García Márquez' where `uid` = 10001;
update check_gho_t set name = 'One Hundred Years of Solitude' where name = 'Cien años de soledad';
alter table check_gho_t add column age int;
alter table check_gho_t add key name (name);
insert into check_gho_t (uid, name, info) values (10004, 'Buenos Aires', '{"age": 10}');
alter table check_gho_t add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
insert into check_gho_t (uid, name, info) values (10005, 'Buenos Aires', '{"age": 100}');
