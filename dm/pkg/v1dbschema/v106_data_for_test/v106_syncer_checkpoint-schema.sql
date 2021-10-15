CREATE TABLE IF NOT EXISTS `dm_meta_v106_test`.`test_syncer_checkpoint` (
    `id` varchar(32) NOT NULL,
    `cp_schema` varchar(128) NOT NULL,
    `cp_table` varchar(128) NOT NULL,
    `binlog_name` varchar(128) DEFAULT NULL,
    `binlog_pos` int(10) unsigned DEFAULT NULL,
    `is_global` tinyint(1) DEFAULT NULL,
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_id_schema_table` (`id`,`cp_schema`,`cp_table`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
