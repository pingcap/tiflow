CREATE TABLE IF NOT EXISTS `dm_meta_v106_test`.`test_onlineddl` (
                                  `id` varchar(32) NOT NULL,
                                  `ghost_schema` varchar(128) NOT NULL,
                                  `ghost_table` varchar(128) NOT NULL,
                                  `ddls` text DEFAULT NULL,
                                  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                  UNIQUE KEY `uk_id_schema_table` (`id`,`ghost_schema`,`ghost_table`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
