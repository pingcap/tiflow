create database `sharding2`;
use `sharding2`;
CREATE TABLE `t4` (`id` bigint(20) AUTO_INCREMENT, `uid` int(11), `name` varchar(80), `info` varchar(100), `age` int(11), `info_json` json GENERATED ALWAYS AS (`info`) VIRTUAL, `id_gen` int(11) GENERATED ALWAYS AS ((`uid` + 1)) VIRTUAL, create_by DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00', PRIMARY KEY (`id`), UNIQUE KEY `uid` (`uid`), UNIQUE KEY `id_gen` (`id_gen`), KEY `multi_col_idx` (`uid`,`id_gen`)) DEFAULT CHARSET=utf8mb4;
insert into `sharding2`.t4 (uid, name, info) values (70001, 'golang', '{"age": 6}'), (70002, 'rust con', '{"age": 1}'), (70003, 'javascript', '{"key": "value", "age": 20}');
insert into `sharding1`.t2 (uid, name, info) values (50002, 'tyuil', '{"ghjkl;": "as a standby"}'), (50003, 'xxxxx', '{"boolean": false, "k": "v"}');
insert into `sharding1`.t3 (uid, name, info) values (30004, 'hhhhhh', '{"key1": "value000"}');
