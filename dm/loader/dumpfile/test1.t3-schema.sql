/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;

CREATE TABLE `binlog_1` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT,
  `t_json` VARCHAR(100),
  `t_json_gen` json comment 'test comment' as (`t_json`) VIRTUAL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
