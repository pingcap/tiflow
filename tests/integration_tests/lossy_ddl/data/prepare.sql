DROP DATABASE IF EXISTS `lossy_ddl`;
CREATE DATABASE `lossy_ddl`;
USE `lossy_ddl`;

CREATE TABLE example
(
    id INT PRIMARY KEY,
    b  INT
);
INSERT INTO example (id, b)
VALUES (1, 1);
ALTER TABLE example MODIFY COLUMN b INT UNSIGNED;