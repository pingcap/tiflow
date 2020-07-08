drop database if exists `row_format`;
create database `row_format`;
use `row_format`;
CREATE TABLE `mars_task_verify_statistics` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `resource_id` varchar(24) NOT NULL,
  `task_id` bigint(20) NOT NULL,
  `source` varchar(36) NOT NULL,
  `category` varchar(26) DEFAULT NULL,
  `priority` smallint(6) DEFAULT 0,
  `type` smallint(6) NOT NULL,
  `task_time` timestamp NOT NULL,
  `start_time` timestamp NULL DEFAULT NULL,
  `end_time` timestamp NULL DEFAULT NULL,
  `reason` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_resource_id` (`resource_id`),
  UNIQUE KEY `u_source_task` (`source`, `task_id`),
  KEY `idx_task_time` (`task_time`),
  KEY `idx_end_time` (`end_time`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;
insert into `mars_task_verify_statistics` (
    `resource_id`,
    `task_id`,
    `source`,
    `category`,
    `priority`,
    `type`,
    `task_time`,
    `start_time`,
    `end_time`,
    `reason`
  )
VALUES (
    '11',
    1,
    '11',
    '11',
    1,
    1,
    '2020-01-01 00:00:00',
    '2020-01-01 00:00:00',
    '2020-01-01 00:00:00',
    '11'
  );
insert into `mars_task_verify_statistics` (
    `resource_id`,
    `task_id`,
    `source`,
    `category`,
    `priority`,
    `type`,
    `task_time`,
    `start_time`,
    `end_time`,
    `reason`
  )
VALUES (
    '22',
    2,
    '22',
    '22',
    2,
    2,
    '2020-01-01 22:00:00',
    '2020-01-01 22:00:00',
    '2020-01-01 22:00:00',
    '22'
  );

delete from `mars_task_verify_statistics` where `id` = 1;