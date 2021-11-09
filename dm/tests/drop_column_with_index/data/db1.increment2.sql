use drop_column_with_index;

create table tb(id int primary key, col int);
create index idx_col on tb(col);
alter table tb drop column col, drop index idx_col;
