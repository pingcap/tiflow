drop database if exists `dmctl`;
create database `dmctl`;
use `dmctl`;
create table t_1(id bigint auto_increment, b int, c varchar(20), d varchar(10), primary key id(id), unique key b(b));
create table t_2(id bigint auto_increment, b int, c varchar(20), d varchar(10), primary key id(id), unique key b(b));
INSERT INTO `dmctl`.`t_2` (`b`,`c`,`d`,`id`) VALUES (1795844527,'mpNYtz','JugWqaHw',1201);
INSERT INTO `dmctl`.`t_2` (`b`,`c`,`d`,`id`) VALUES (816772144,'jjoPwqhBWpJyUUvgGWkp','FgPbiUqrvS',1202);
INSERT INTO `dmctl`.`t_2` (`b`,`c`,`d`,`id`) VALUES (1058572812,'dCmAIAuZrNUJxBl','wiaFgp',1203);
INSERT INTO `dmctl`.`t_1` (`b`,`c`,`d`,`id`) VALUES (1825468799,'DWzgtMAwUcoqZvupwm','GsusfUlbB',1101);
INSERT INTO `dmctl`.`t_1` (`b`,`c`,`d`,`id`) VALUES (265700472,'rEsjuTsIS','JPTd',1102);
INSERT INTO `dmctl`.`t_2` (`b`,`c`,`d`,`id`) VALUES (763390433,'TE','jbO',1204);
INSERT INTO `dmctl`.`t_1` (`b`,`c`,`d`,`id`) VALUES (1112494892,'XDbXXvYTtJFLaF','zByU',1103);
INSERT INTO `dmctl`.`t_2` (`b`,`c`,`d`,`id`) VALUES (61186151,'gXhXNtk','Hi',1205);
INSERT INTO `dmctl`.`t_2` (`b`,`c`,`d`,`id`) VALUES (1190671373,'WGP','jUXxu',1206);
INSERT INTO `dmctl`.`t_2` (`b`,`c`,`d`,`id`) VALUES (1192770284,'SyMVcUeK','MIZNFu',1207);
INSERT INTO `dmctl`.`t_1` (`b`,`c`,`d`,`id`) VALUES (1647531504,'yNvqWnrbtTxc','ogSwAofM',1104);
INSERT INTO `dmctl`.`t_1` (`b`,`c`,`d`,`id`) VALUES (1041099481,'zrO','C',1105);
INSERT INTO `dmctl`.`t_2` (`b`,`c`,`d`,`id`) VALUES (1635431660,'pum','MMtT',1208);
INSERT INTO `dmctl`.`t_1` (`b`,`c`,`d`,`id`) VALUES (208389298,'ZvhKh','Zt',1106);
INSERT INTO `dmctl`.`t_2` (`b`,`c`,`d`,`id`) VALUES (2128788808,'hgWB','poUlMgBSX',1209);
INSERT INTO `dmctl`.`t_2` (`b`,`c`,`d`,`id`) VALUES (1758036092,'CxSfGQNebY','OY',1210);
INSERT INTO `dmctl`.`t_1` (`b`,`c`,`d`,`id`) VALUES (1649664004,'eIXDUjODpLjRkXu','NWlGjQq',1107);
INSERT INTO `dmctl`.`t_1` (`b`,`c`,`d`,`id`) VALUES (1402446429,'xQMCGsfckXpoe','R',1108);
INSERT INTO `dmctl`.`t_1` (`b`,`c`,`d`,`id`) VALUES (800180420,'JuUIxUacksp','sX',1109);

create table tb_1(a INT, b INT);
create table tb_2(a INT, c INT);
create table _tb_1_gho(a INT, b INT); -- trigger online ddl checker

create table precheck_optimistic_tb_1(a INT, b INT, primary key a(a));
create table precheck_optimistic_tb_2(a INT, c INT, primary key a(a));

CREATE TABLE only_warning (id bigint, b int, primary key id(id), FOREIGN KEY (b) REFERENCES t_1(b));
