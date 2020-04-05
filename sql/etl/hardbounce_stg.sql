drop table if exists {{ params.stg_table }};
select 
	emaildomain_cat
	,emaildomainpiece1
	,emaildomainpiece2
	,cast(case when regdate_n = '' then null else regdate_n end as date) as regdate_n
	,cast(case when birthdate_n = '' then null else birthdate_n end as date) as birthdate_n 
	,cast(monthssinceregdate as int) as monthssinceregdate
	,cast(age as int) as age
	,cast(percnumbersinemailuser as numeric(10,2)) as percnumbersinemailuser
	,cast(hasnumberinemailuser as int) as hasnumberinemailuser
	,cast(emailusercharqty as int) as emailusercharqty
	,cast(flghardbounce_n as int) as flghardbounce_n
	,cast(now() as date) as timestamp
into
	{{ params.stg_table }}
from 
	{{ params.raw_table }};