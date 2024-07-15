use metric_spike;

# Weekly user engagement
with weekly as(
select
week(occurred_at) as Week,
count(distinct user_id) as `Weekly user Engagement`
from events
group by week(occurred_at)
order by week(occurred_at)
)
select
Week,
`Weekly User Engagement`,
(select avg(`Weekly User Engagement`) 
from weekly) as `Average Weekly User Engagement`
from weekly
order by Week;

# User growth analysis
with growth as(
select 
   year(created_at) as `Year`,
   week(created_at) as `Week`,
   count(user_id) as `Active users`
   from users
   group by `Year`, `Week`
   )
   select
     growth.`Year`,
     growth.`Week`,
     growth.`Active users`,
     sum(growth.`Active users`)
     over (order by growth.`Year`, growth.`Week`) as `Active user growth`
     from growth;
     
# Weekly retention analysis
with retention1 as(
select 
    count(distinct events.user_id) as `user signup`, 
	week(occurred_at) as `week`, 
    year(occurred_at) as year
from users
inner join events
on users.user_id = events.user_id
where event_name = 'complete_signup' and users.activated_at is not null
group by `week`, year
),
retention2 as(
Select 
    count(distinct events.user_id) as `user retained`, 
	week(occurred_at) as `week` 
from users
inner join events
on users.user_id = events.user_id
where event_name = 'login' and users.activated_at is not null
group by `week`
)
select retention1.year,
	   retention2.`week`,
       retention1.`user signup`,
       retention2.`user retained`
from retention1
inner join retention2 on
retention1.`week` = retention2.`week`;

# Weekly Engagement per device
select
 week(occurred_at) as `Week`,
 device,
 count(distinct user_id) as `Device engagement`
from events
group by device, `Week`
order by `Week`, `Device engagement` desc;


# Email enagagement analysis
with email as(
select distinct week(occurred_at) as `week`,
count(distinct case when action = 'sent_weekly_digest' then user_id end) as `weekly digest`,
count(distinct case when action ='email_open' then user_id end) as `open email`,
count(distinct case when action = 'email_clickthrough' THEN user_id end) as `click email`,
count(distinct case when action='sent_reengagement_email' then user_id end) as `reengagement email`
from email_events
group by `week`
)
select
round(avg(email.`weekly digest`)) as `Average weekly digest`,
round(avg(email.`open email`)) as `Average opened email`,
round(avg(email.`click email`)) as `Average email clicked`,
round(avg(email.`reengagement email`)) as `Average email reengagement`
from email;



