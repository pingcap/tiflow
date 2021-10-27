drop database if exists `checkpoint_transaction`;
create database `checkpoint_transaction`;
use `checkpoint_transaction`;

create table t1 (
    id int NOT NULL AUTO_INCREMENT,
    a int,
    b int,
    PRIMARY KEY (id)
);
insert into t1(a) values (0);

delimiter $$
drop procedure if exists dowhile1; 
create procedure dowhile1(nums int)
begin
WHILE nums > 0 DO
    insert into t1(a) values(nums);
    set nums = nums - 1;
END WHILE;
end $$

drop procedure if exists dowhile2; 
create procedure dowhile2(nums int)
begin
WHILE nums > 0 DO
    insert into t1(b) values(nums);
    set nums = nums - 1;
END WHILE;
end $$

delimiter ;