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


// 5. Total working days for each agents 

select Agent as name, count(distinct date_dd) as total_working_Days from LoginReport group by Agent;

//6. Total query that each agent have taken 

 select Agent_name as name, sum(Total_chats) as total_query_take from AgentPerfromance group by Agent_name order by total_query_take desc;

//7. Total Feedback that each agent have received 

select Agent_name as name, sum(total_feedback) as total_feedback_taken from AgentPerfromance group by Agent_name order by total_feedback_taken desc;

//8. Agent name who have average rating between 3.5 to 4 

select Agent_name from AgentPerfromance where Avg_rating BETWEEn 3.5 and 4;

//9. Agent name who have rating less than 3.5 

select Agent_name from AgentPerfromance where Avg_rating < 3.5;

//10. Agent name who have rating more than 4.5 
select Agent_name form AgentPerfromance where Avg_rating > 4.5;


//-11. How many feedback agents have received more than 4.5 
select count(total_feedback from AgentPerfromance where total_feedback > 4.5;

//12. average weekly response time for each agent 

SELECT
    Agent_name AS agentName,
       CONCAT('Week ', (WEEKOFYEAR(Date_d) - WEEKOFYEAR(DATE_SUB(Date_d, DAYOFMONTH(Date_d)))) + 1) AS weekNo,
       ROUND(AVG(CAST(Avg_resolution AS double))/60, 2) AS weeklyAvgResolutionTimeInMinutes
     FROM
       AgentPerfromance
     WHERE
       Total_chats <> 0
     GROUP BY
       Agent_name,
       CONCAT('Week ', (WEEKOFYEAR(Date_d) - WEEKOFYEAR(DATE_SUB(Date_d, DAYOFMONTH(Date_d)))) + 1)
     ORDER BY
       Agent_name,
    weekNo;

//13. average weekly resolution time for each agents 
SELECT
  Agent_name AS agentName,
  CONCAT('Week ', (WEEKOFYEAR(Date_d) - WEEKOFYEAR(DATE_SUB(Date_d, DAYOFMONTH(Date_d)))) + 1) AS weekNo,
  ROUND(AVG(CAST(Avg_resolution AS double))/60, 2) AS weeklyAvgResolutionTimeInMinutes
FROM
  AgentPerfromance
WHERE
  Total_chats <> 0
GROUP BY
  Agent_name,
  CONCAT('Week ', (WEEKOFYEAR(Date_d) - WEEKOFYEAR(DATE_SUB(Date_d, DAYOFMONTH(Date_d)))) + 1)
ORDER BY
  Agent_name,
  weekNo;


//14. Find the number of chat on which they have received a feedback 
select Agent_name as Agent_name, count(dictinct Sl_no) as numberofchatswithfeddback from AgentPerfromance where total_feedback > 0 group by Agent_name;

//15. Total contribution hour for each and every agents weekly basis 
SELECT
  Agent_name AS agentName,
  CONCAT('Week ', (WEEKOFYEAR(Date_d) - WEEKOFYEAR(DATE_SUB(Date_d, DAYOFMONTH(Date_d)))) + 1) AS weekNo,
  ROUND(SUM(Total_chats * (CAST(Avg_responce_time AS double) + CAST(Avg_resolution AS double))) / 3600, 2) AS totalContributionHours
FROM
  AgentPerfromance
GROUP BY
  Agent_name,
  CONCAT('Week ', (WEEKOFYEAR(Date_d) - WEEKOFYEAR(DATE_SUB(Date_d, DAYOFMONTH(Date_d)))) + 1)
ORDER BY
  Agent_name,
  weekNo;


//16. Perform inner join, left join and right join based on the agent column and after joining the table export that data into your --local system.

CREATE TABLE agent_data_inner_join
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED as textfile
AS
select
'SL No' as serialNo
,'Agent' as name
,'Date' as date
,'Login Time' as loginTime
,'Logout Time' as logoutTime
,'Duration' as duration
,'Performance SL No' as pSerialNo
,'Performance Date' as pDate
,'Total Chats' as totalChats
,'Average Response Time' as avgResponseTime
,'Average Resolution Time' as avgResolutionTime
,'Average Rating' as avgRating
,'Total Feedback' as totalfeedback;

insert into agent_data_inner_join
select
alr.serialNo, 
alr.name as agentName,
from_unixtime(unix_timestamp(alr.date), 'dd-MMM-yy') as loginDate,
from_unixtime(unix_timestamp(alr.loginTime), 'HH:mm:ss') as loginTime,
from_unixtime(unix_timestamp(alr.logoutTime), 'HH:mm:ss') as logoutTime,
concat(cast(floor(alr.duration/3600) as int),':',lpad(cast(floor((alr.duration%3600)/60) as int),2,'0'),':',lpad(cast(floor(alr.duration%60) as int),2,'0')) as duration,
ap.serialNo as pSerialNo,
from_unixtime(unix_timestamp(ap.date), 'M/dd/yyyy') as performanceDate,
ap.totalChats,
concat(cast(floor(ap.avgResponseTime/3600) as int),':',lpad(cast(floor((ap.avgResponseTime%3600)/60) as int),2,'0'),':',lpad(cast(floor(ap.avgResponseTime%60) as int),2,'0')) as averageResponseTime,
concat(cast(floor(ap.avgResolutionTime/3600) as int),':',lpad(cast(floor((ap.avgResolutionTime%3600)/60) as int),2,'0'),':',lpad(cast(floor(ap.avgResolutionTime%60) as int),2,'0')) as averageResolutionTime,
avgRating,
totalFeedback
from agent_logging_report alr
join agent_performance ap on lower(trim(alr.name)) = lower(trim(ap.name));

hadoop fs -cat hdfs://quickstart.cloudera:8020/user/hive/warehouse/agent_data_inner_join/* > ~/agent_data_inner_join.csv

CREATE TABLE agent_data_left_join
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED as textfile
AS
select
'SL No' as serialNo
,'Agent' as name
,'Date' as date
,'Login Time' as loginTime
,'Logout Time' as logoutTime
,'Duration' as duration
,'Performance SL No' as pSerialNo
,'Performance Date' as pDate
,'Total Chats' as totalChats
,'Average Response Time' as avgResponseTime
,'Average Resolution Time' as avgResolutionTime
,'Average Rating' as avgRating
,'Total Feedback' as totalfeedback;

insert into agent_data_left_join
select
alr.serialNo, 
alr.name as agentName,
from_unixtime(unix_timestamp(alr.date), 'dd-MMM-yy') as loginDate,
from_unixtime(unix_timestamp(alr.loginTime), 'HH:mm:ss') as loginTime,
from_unixtime(unix_timestamp(alr.logoutTime), 'HH:mm:ss') as logoutTime,
concat(cast(floor(alr.duration/3600) as int),':',lpad(cast(floor((alr.duration%3600)/60) as int),2,'0'),':',lpad(cast(floor(alr.duration%60) as int),2,'0')) as duration,
ap.serialNo as pSerialNo,
from_unixtime(unix_timestamp(ap.date), 'M/dd/yyyy') as performanceDate,
ap.totalChats,
concat(cast(floor(ap.avgResponseTime/3600) as int),':',lpad(cast(floor((ap.avgResponseTime%3600)/60) as int),2,'0'),':',lpad(cast(floor(ap.avgResponseTime%60) as int),2,'0')) as averageResponseTime,
concat(cast(floor(ap.avgResolutionTime/3600) as int),':',lpad(cast(floor((ap.avgResolutionTime%3600)/60) as int),2,'0'),':',lpad(cast(floor(ap.avgResolutionTime%60) as int),2,'0')) as averageResolutionTime,
avgRating,
totalFeedback
from agent_logging_report alr
left join agent_performance ap on lower(trim(alr.name)) = lower(trim(ap.name));

hadoop fs -cat hdfs://quickstart.cloudera:8020/user/hive/warehouse/agent_data_left_join/* > ~/agent_data_left_join.csv

CREATE TABLE agent_data_right_join
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED as textfile
AS
select
'SL No' as serialNo
,'Agent' as name
,'Date' as date
,'Login Time' as loginTime
,'Logout Time' as logoutTime
,'Duration' as duration
,'Performance SL No' as pSerialNo
,'Performance Date' as pDate
,'Total Chats' as totalChats
,'Average Response Time' as avgResponseTime
,'Average Resolution Time' as avgResolutionTime
,'Average Rating' as avgRating
,'Total Feedback' as totalfeedback;

insert into agent_data_right_join
select
alr.serialNo, 
alr.name as agentName,
from_unixtime(unix_timestamp(alr.date), 'dd-MMM-yy') as loginDate,
from_unixtime(unix_timestamp(alr.loginTime), 'HH:mm:ss') as loginTime,
from_unixtime(unix_timestamp(alr.logoutTime), 'HH:mm:ss') as logoutTime,
concat(cast(floor(alr.duration/3600) as int),':',lpad(cast(floor((alr.duration%3600)/60) as int),2,'0'),':',lpad(cast(floor(alr.duration%60) as int),2,'0')) as duration,
ap.serialNo as pSerialNo,
from_unixtime(unix_timestamp(ap.date), 'M/dd/yyyy') as performanceDate,
ap.totalChats,
concat(cast(floor(ap.avgResponseTime/3600) as int),':',lpad(cast(floor((ap.avgResponseTime%3600)/60) as int),2,'0'),':',lpad(cast(floor(ap.avgResponseTime%60) as int),2,'0')) as averageResponseTime,
concat(cast(floor(ap.avgResolutionTime/3600) as int),':',lpad(cast(floor((ap.avgResolutionTime%3600)/60) as int),2,'0'),':',lpad(cast(floor(ap.avgResolutionTime%60) as int),2,'0')) as averageResolutionTime,
avgRating,
totalFeedback
from agent_logging_report alr
left join agent_performance ap on lower(trim(alr.name)) = lower(trim(ap.name));

hadoop fs -cat hdfs://quickstart.cloudera:8020/user/hive/warehouse/agent_data_right_join/* > ~/agent_data_right_join.csv


//17. Perform partitioning on top of the agent column and then on top of that perform bucketing for each partitioning.

CREATE TABLE agent_data_partitioned_clustered
( 
serialNo INT,
date string,
loginTime string,
logoutTime string,
duration string,
pSerialNo int,
pDate string,
totalChats int,
avgResponseTime string,
avgResolutionTime string,
avgRating decimal(3,2),
totalfeedback int
)
partitioned by(name string)
clustered by(serialNo) into 5 buckets;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing = true;

insert into agent_data_partitioned_clustered partition(name)
select
alr.serialNo, 
from_unixtime(unix_timestamp(alr.date), 'dd-MMM-yy') as loginDate,
from_unixtime(unix_timestamp(alr.loginTime), 'HH:mm:ss') as loginTime,
from_unixtime(unix_timestamp(alr.logoutTime), 'HH:mm:ss') as logoutTime,
concat(cast(floor(alr.duration/3600) as int),':',lpad(cast(floor((alr.duration%3600)/60) as int),2,'0'),':',lpad(cast(floor(alr.duration%60) as int),2,'0')) as duration,
ap.serialNo as pSerialNo,
from_unixtime(unix_timestamp(ap.date), 'M/dd/yyyy') as performanceDate,
ap.totalChats,
concat(cast(floor(ap.avgResponseTime/3600) as int),':',lpad(cast(floor((ap.avgResponseTime%3600)/60) as int),2,'0'),':',lpad(cast(floor(ap.avgResponseTime%60) as int),2,'0')) as averageResponseTime,
concat(cast(floor(ap.avgResolutionTime/3600) as int),':',lpad(cast(floor((ap.avgResolutionTime%3600)/60) as int),2,'0'),':',lpad(cast(floor(ap.avgResolutionTime%60) as int),2,'0')) as averageResolutionTime,
avgRating,
totalFeedback,
alr.name
from agent_logging_report alr
join agent_performance ap on lower(trim(alr.name)) = lower(trim(ap.name));

