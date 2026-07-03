drop database if exists `test`;
create database `test`;
use `test`;

CREATE TABLE `update_pk` (
  `id` int PRIMARY KEY NONCLUSTERED,
  `pad` varchar(100) NOT NULL
);
INSERT INTO `update_pk` (`id`, `pad`) VALUES (1, 'example1'), (2, 'example2');
INSERT INTO `update_pk` (`id`, `pad`) VALUES (10, 'example10'), (20, 'example20');
INSERT INTO `update_pk` (`id`, `pad`) VALUES (100, 'example100');
INSERT INTO `update_pk` (`id`, `pad`) VALUES (1000, 'example1000');


SHOW INDEX FROM update_pk;

CREATE TABLE `update_uk` (
  `id` int PRIMARY KEY NONCLUSTERED,
  `uk` int NOT NULL,
  `pad` varchar(100) NOT NULL,
  UNIQUE KEY `uk` (`uk`)
);
INSERT INTO `update_uk` (`id`, `uk`, `pad`) VALUES (1, 1, 'example1'), (2, 2, 'example2');
INSERT INTO `update_uk` (`id`, `uk`, `pad`) VALUES (10, 10, 'example10'), (20, 20, 'example20');
INSERT INTO `update_uk` (`id`, `uk`, `pad`) VALUES (100, 100, 'example100');
INSERT INTO `update_uk` (`id`, `uk`, `pad`) VALUES (1000, 1000, 'example1000');

SHOW INDEX FROM update_uk;