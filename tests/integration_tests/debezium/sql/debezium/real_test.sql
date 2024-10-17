-- set sql_mode REAL_AS_FLOAT is necessary
set @@session.sql_mode=concat(@@session.sql_mode, ',REAL_AS_FLOAT');
CREATE TABLE `REAL_TEST` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `r1` real default 1.25,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
INSERT INTO REAL_TEST(r1) VALUE (2.36);