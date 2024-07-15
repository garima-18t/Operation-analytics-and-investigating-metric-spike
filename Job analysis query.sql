create database if not exists jobanalysis;
use jobanalysis;

#create table for jobdata

create table job_data (
   ds date,
   job_id int not null,
   actor_id int not null,
   event varchar(30),
   language varchar(30),
   time_spent int,
   org varchar(5)
   );
   
   #insert values into the table

insert into job_data (ds, job_id, actor_id, event, language, time_spent, org)
values
('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
('2020-11-28', 23, 1005, 'transfer', 'Persian', 22, 'D'),
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');

#Performing tasks

#Job reviewed over time

select ds as date,
round((count(job_id)/ sum(time_spent))*3600) as `Jobs reviewed`
from job_data
where ds between '2020-11-01' and '2020-11-30'
group by ds;

#Throughput analysis
with base as(
select ds as `date`,
round((count(event)/ sum(time_spent)),2) as `Daily metric`
from job_data
group by ds
)
select `date`, base.`Daily metric`,
       avg(base.`Daily metric`) 
       over(order by `date` rows between 6 preceding and current row)
       as `7 day rolling average`
       from base;

#Language share analysis

select language,
    count(language) as `language count`,
    round((count(language) * 100/
    (select count(*) from job_data where ds between '2020-11-01' and '2020-11-30')), 2) as `percentage share`
from job_data
group by language
order by language desc;

# Duplicate rows detection based on single column
select actor_id, 
count(actor_id) as tot_count 
from job_data
group by actor_id 
having tot_count>1;

# Duplicate rows detection first query

select ds, job_id, actor_id, event, language, time_spent, org, 
count(*) as `Total Duplicate rows`
from job_data
group by ds, job_id, actor_id, event, language, time_spent, org
having count(*) > 1;

#duplicate rows detection 

with dup as (
select *, row_number()
over(partition by ds,job_id,actor_id,event,language,time_spent,org)
as  `No. of Rows` from job_data
)
select *,
case when dup.`No. of Rows`=1 then "No Duplicate" 
else "Duplicate" end as Duplicate
from dup;


#delete duplicates ( at first instead of returning 8 rows i was getting 32 rows so to fix that)

DELETE FROM job_data
WHERE (ds, job_id, actor_id, event, language, time_spent, org) IN (
    SELECT ds, job_id, actor_id, event, language, time_spent, org
    FROM (
        SELECT ds, job_id, actor_id, event, language, time_spent, org,
               ROW_NUMBER() OVER (PARTITION BY ds, job_id, actor_id, event, language, time_spent, org ORDER BY ds DESC) AS rn
        FROM job_data
    ) AS sub
    WHERE rn > 1
);

   SET SQL_SAFE_UPDATES = 0; #disable safe update mode for the current MySQL session
