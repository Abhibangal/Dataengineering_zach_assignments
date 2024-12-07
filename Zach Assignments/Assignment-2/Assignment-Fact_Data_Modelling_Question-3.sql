/*
 A cumulative query to generate device_activity_datelist from events
 */

with get_date as
--get the yesterday and today's date value my referring user_devices_cumulated
    (
        select
                coalesce(max(date),date('2022-12-31')) as yesterday,
                (coalesce(max(date),date('2022-12-31'))  + interval '1 day' )::date  as today
            from user_devices_cumulated
    ),
-- using get_date  cte extract yesterday's data from user_devices_cumulated table
    yesterday_data as
    (
        select a.* from
                (
                select user_id,
                                browser_type,
                                device_actives,
                                date
                         from user_devices_cumulated
                         where date = (select yesterday from get_date)
                )a
    ),
-- using get_date  cte extract today's distinct data from events table based on user_id,browser_type,device_actives date
    today_distinct_data as
    (
        select distinct
            e.user_id  as user_id,
            d.browser_type as browser_type,
            date(cast(e.event_time as timestamp)) as device_actives,
            date(cast(e.event_time as timestamp))as date
        from events e
        inner join devices d
        on d.device_id = e.device_id
        where   e.device_id is not null
            and e.user_id is not null
            and  date(cast(e.event_time as timestamp)) = (select today from get_date)

    ),
-- using today_data  cte create array by aggregating on user_id , browser_type and date
    today_data as (
                select user_id,
                       browser_type,
                       array_agg(device_actives) as device_actives,
                       date
                from today_distinct_data
                group by user_id, browser_type, date
                ),
-- using yesterday's data and today's dat create a cumulated data
    cumulated_data as (
                select
                coalesce(t.user_id,y.user_id)as user_id,
                coalesce(t.browser_type,y.browser_type) as browser_type,
                case    when y.user_id is null
                           then t.device_actives
                        when y.user_id is not null
                            then  t.device_actives || y.device_actives
                        end as device_actives,
                coalesce(t.date,(y.date + interval '1 day')::date) as date
                from today_data t
                full outer join  yesterday_data y
                on  y.user_id = t.user_id
                and y.browser_type = t.browser_type
                )
--insert into user_devices_cumulated using on conflict
insert into user_devices_cumulated
select
       user_id,
        browser_type,
        device_actives,
        date
from cumulated_data a
on conflict (user_id,browser_type,date)
do update
    set device_actives = excluded.device_actives;

