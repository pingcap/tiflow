use `safe_mode`;
-- update non key column
update t set a = "hello2" where id = 1;
update t set a = "world2" where id = 2;
