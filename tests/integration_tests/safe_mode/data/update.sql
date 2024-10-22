-- update non key column
update `safe_mode.t` set a = "hello2" where id = 1;
update `safe_mode.t` set a = "world2" where id = 2;