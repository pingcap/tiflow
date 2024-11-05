set @@foreign_key_checks=0;
create database `NextTimeTable`;
use `NextTimeTable`;
create table NextTimeTable.REFERENCED (
     SUBJECT_ID int not null,
     PRIMARY KEY (SUBJECT_ID)
     );

create table `NextTimeTable`.`TIMETABLE_SUBJECT_GROUP_MAPPING` (
     pk1 int not null,
     `SUBJECT_ID` int not null,
     `other` int,
     CONSTRAINT `FK69atxmt7wrwpb4oekyravsx9l` FOREIGN KEY (`SUBJECT_ID`) REFERENCES `NextTimeTable`.`REFERENCED`(`SUBJECT_ID`)
     );

Alter table `NextTimeTable`.`TIMETABLE_SUBJECT_GROUP_MAPPING`
drop foreign key `FK69atxmt7wrwpb4oekyravsx9l`;
Alter table `NextTimeTable`.`TIMETABLE_SUBJECT_GROUP_MAPPING`
drop index `FK69atxmt7wrwpb4oekyravsx9l`;
Alter table `NextTimeTable`.`TIMETABLE_SUBJECT_GROUP_MAPPING`
drop column `SUBJECT_ID`;

create table `NextTimeTable`.`table1` ( pk1 int not null PRIMARY KEY, `id` int not null, `other` int );
