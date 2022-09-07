drop user if exists 'dm_user'@'%';
create user 'dm_user'@'%' require X509;
grant all privileges on *.* to 'dm_user'@'%';

drop database if exists `tls`;
create database `tls`;
use `tls`;

create table `t2` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`id`)
);
insert into `t2` values (1);
