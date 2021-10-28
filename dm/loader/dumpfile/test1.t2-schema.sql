/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;

CREATE TABLE "binlog_1" (
  "id" bigint(11) NOT NULL AUTO_INCREMENT,
  "t_boolean" tinyint(1) DEFAULT NULL,
  "t_bigint" bigint(20) DEFAULT NULL,
  "t_double" double DEFAULT NULL,
  "t_decimal" decimal(38,19) DEFAULT NULL,
  "t_bit" bit(64) DEFAULT NULL,
  "t_date" date DEFAULT NULL,
  "t_datetime" datetime DEFAULT NULL,
  "t_timestamp" timestamp NULL DEFAULT NULL,
  "t_time" time DEFAULT NULL,
  "t_year" year(4) DEFAULT NULL,
  "t_char" char(1) DEFAULT NULL,
  "t_varchar" varchar(10) DEFAULT NULL,
  "t_blob" blob,
  "t_text" text,
  "t_enum" enum('enum1','enum2','enum3') DEFAULT NULL,
  "t_set" set('a','b','c') DEFAULT NULL,
  "t_json" json DEFAULT NULL,
  PRIMARY KEY ("id")
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
