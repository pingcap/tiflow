create database if not exists `dm_meta`;
CREATE TABLE IF NOT EXISTS `dm_meta`.`test_syncer_checkpoint` (
    `id` varchar(32) NOT NULL,
    `cp_schema` varchar(128) NOT NULL,
    `cp_table` varchar(128) NOT NULL,
    `binlog_name` varchar(128) DEFAULT NULL,
    `binlog_pos` int(10) unsigned DEFAULT NULL,
    `is_global` tinyint(1) DEFAULT NULL,
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_id_schema_table` (`id`,`cp_schema`,`cp_table`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
INSERT INTO `dm_meta`.`test_syncer_checkpoint` VALUES
('mysql-replica-01','','','BINLOG_NAME1',BINLOG_POS1,1,'2020-07-31 18:10:40','2020-07-31 18:11:40'),
('mysql-replica-01','import_v10x','t1','BINLOG_NAME1',BINLOG_POS1,0,'2020-07-31 18:10:40','2020-07-31 18:11:40'),
('mysql-replica-02','','','BINLOG_NAME2',BINLOG_POS2,1,'2020-07-31 18:10:40','2020-07-31 18:11:40'),
('mysql-replica-02','import_v10x','t2','BINLOG_NAME2',BINLOG_POS2,0,'2020-07-31 18:10:40','2020-07-31 18:11:40');
