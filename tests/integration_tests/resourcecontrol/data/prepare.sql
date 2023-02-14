drop database if exists `resourcecontrol`;
create database `resourcecontrol`;
use `resourcecontrol`;

SET GLOBAL tidb_enable_resource_control='on';
create resource group rg1 ru_per_sec=1000;
alter resource group rg1 ru_per_sec=2000, burstable;
drop resource group rg1;
SET GLOBAL tidb_enable_resource_control='off';

create table finish_mark (id int PRIMARY KEY);

