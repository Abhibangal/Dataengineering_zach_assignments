/*
The incremental query to generate host_activity_datelist
 */
insert into hosts_cumulated
with get_date as
--get the yesterday and today's date value my referring hosts_cumulated

    (
    select coalesce(max(date),'2023-01-01') as yesterday,
           coalesce(max(date)+1,'2023-01-01') as today
    from hosts_cumulated
    ),
-- using yesterday_data  cte extract yesterday's data from hosts_cumulated table
    yesterday_data as
    (
        select
            host,
            host_activity_datelist,
            date
        from hosts_cumulated
        where date = (select yesterday from get_date)

    ),
-- using get_date  cte extract today's distinct data from events table based on host and date
    today_distinct_data as
    (
        select
            distinct
            host as host,
            cast(event_time :: timestamp as date) as host_activity,
            cast(event_time :: timestamp as date) as date
        from events
        where cast(event_time :: timestamp as date)  = (select today from get_date)
    ),
-- using today_data  cte create array by aggregating on user_id , browser_type and date
    today_data as
    (
        select
            host,
            array[today_distinct_data.host_activity] as host_activity_datelist,
            date
        from today_distinct_data
    )
-- using yesterday's data and today's data and  created a hosts_cumulated data
select coalesce(t.host, y.host)as host,
       case
           when y.host is null
               then t.host_activity_datelist
           when y.host is not null
               then t.host_activity_datelist || y.host_activity_datelist
           end  as host_activity_datelist,
       coalesce(t.date, (y.date + interval '1 day')):: date as date
from today_data t
         full outer join yesterday_data y
                         on y.host = t.host
on conflict (host, date)
do update
    set host_activity_datelist = excluded.host_activity_datelist;


-- select * from hosts_cumulated where date = '2023-01-31';
--
-- select array[]::integer[] || array[1,2] ;