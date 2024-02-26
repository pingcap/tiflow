CREATE TABLE `DBZ5236` (
  id int(11) not null primary key auto_increment,
  ti1 tinyint(3) unsigned NOT NULL DEFAULT '0',
  ti2 tinyint(1) unsigned NOT NULL DEFAULT '0',
  ti3 tinyint(1) NOT NULL DEFAULT '1'
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

INSERT INTO DBZ5236 VALUES (DEFAULT, 1, 1, 0);
