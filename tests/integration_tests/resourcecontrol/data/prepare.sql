drop database if exists `resourcecontrol`;
create database `resourcecontrol`;
use `resourcecontrol`;

create resource group rg1 ru_per_sec=1000;
alter resource group rg1 ru_per_sec=2000, burstable;
drop resource group rg1;

create table finish_mark (id int PRIMARY KEY);

