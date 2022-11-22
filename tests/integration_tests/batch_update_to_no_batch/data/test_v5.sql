-- test add and drop columns
USE `batch_update_to_no_batch`;

CREATE TABLE `add_and_drop_columns`
(
    `id` int(11) NOT NULL PRIMARY KEY
);

insert into `add_and_drop_columns` (id)
values (1);

alter table `add_and_drop_columns`
    add col1 int null,
    add col2 int null,
    add col3 int null;

insert into `add_and_drop_columns` (id, col1, col2, col3)
values (2, 3, 4, 5);

insert into `add_and_drop_columns` (id) values (3);

alter table `add_and_drop_columns`
    drop col1,
    drop col2;

insert into `add_and_drop_columns` (id, col3)
values (4, 5);
