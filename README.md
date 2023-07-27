# hive_project
hive project by ineuron


show databases;

create database hive_proj;

use hive_proj;

create table AgentlogRepo
(
emp_no int,
Agent_name string,
Date_join date,
Login_time string,
Logout_time string,
Duration string
)
row format delimited
fields terminated by ','
tblproperties("skip.header.line.count"="1");

describe agentlogrepo;
describe formatted agentlogrepo;

load data local inpath 'file:////config/workspace/AgentLogingReport (1).csv'into table agentlogrepo;

select * from agentlorepo limit 10;
