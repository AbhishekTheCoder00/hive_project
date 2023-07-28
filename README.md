# hive_project
hive project by ineuron


show databases;

create database hive_proj;

use hive_proj;


create table LoginReport
(
SL_No int,
Agent String,
date_dd date,
Login_time string,
Logout_time string,
duration string
)
row format delimited
fields terminated by ',';


 load data local inpath 'file:///config/workspace/AgentLogingReport (1).csv'into table LoginReport;


 select * from LoginReport limit 10;


create table AgentPerfromance
(
Sl_no int,
Date_d string,
Agent_name string,
Total_chats int,
Avg_responce_time string,
Avg_resolution string,
Avg_rating float,
total_feedback int
)
row format delimited
fields terminated by ',';


load data local inpath 'file:///config/workspace/AgentPerformance (3).csv'into table AgentPerfromance;


 select * from AgentPerfromance limit 10;


select Agent from loginreport;

select Avg_rating from AgentPerfromance;


// 5 th remaining

select Agent as name, count(distinct date_dd) as total_working_Days from LoginReport group by Agent;

//6

 select Agent_name as name, sum(Total_chats) as total_query_take from AgentPerfromance group by Agent_name order by total_query_take desc;

//7

select Agent_name as name, sum(total_feedback) as total_feedback_taken from AgentPerfromance group by Agent_name order by total_feedback_taken desc;

//8

select Agent_name from AgentPerfromance where Avg_rating BETWEEn 3.5 and 4;


