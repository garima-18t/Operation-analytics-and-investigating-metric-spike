create database if not exists metric_spike;
use metric_spike;

##TABLE -1 USERS

create table users(
    user_id int,
    created_at	varchar(100),
    company_id int,
    language varchar(50),	
    activated_at varchar(100),	
    state varchar(50)
    );
    
    # to check the path where files need to uploaded
    show variables like 'secure_file_priv';
    
    # to import csv file to mysql
    load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
    into table users
    fields terminated by ','
    enclosed by '"'
    lines terminated by '\n'
    ignore 1 rows;
    
    select * from users;
    
    # to change string type columns to datetype
    alter table users add column temp_created_at datetime;
    update users set temp_created_at = str_to_date(created_at, '%d-%m-%Y %H:%i');
     
     alter table users drop column created_at;
     alter table users change column temp_created_at created_at datetime;
     
     alter table users add column temp_activated_at datetime;
    update users set temp_activated_at = str_to_date(activated_at, '%d-%m-%Y %H:%i');
     
     alter table users drop column activated_at;
     alter table users change column temp_activated_at activated_at datetime;
    
    
    ## TABLE -2 EVENTS
    create table events(
    user_id int,
	occurred_at	varchar(100),
    event_type varchar(50),
    event_name varchar(100),
    location varchar(50),
    device varchar(50),
    user_type int
    );

 # to import csv file to mysql
    load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
    into table events
    fields terminated by ','
    enclosed by '"'
    lines terminated by '\n'
    ignore 1 rows;
    
    select * from events;
    
      # to change string type columns to datetype
    alter table events add column temp_occurred_at datetime;
    update events set temp_occurred_at = str_to_date(occurred_at, '%d-%m-%Y %H:%i');
     
     alter table events drop column occurred_at;
     alter table events change column temp_occurred_at occurred_at datetime;
     
     
     ## TABLE-3 EMAIL EVENTS
     create table email_events(
     user_id int,	
     occurred_at varchar(100),	
     action	varchar(100),
     user_type int
     );

# to import csv file to mysql
    load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
    into table email_events
    fields terminated by ','
    enclosed by '"'
    lines terminated by '\n'
    ignore 1 rows;
    
    select * from email_events;
    
     # to change string type columns to datetype
    alter table email_events add column temp_occurred_at datetime;
    update email_events set temp_occurred_at = str_to_date(occurred_at, '%d-%m-%Y %H:%i');
     
     alter table email_events drop column occurred_at;
     alter table email_events change column temp_occurred_at occurred_at datetime;
    
    
    
