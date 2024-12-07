/*
 An incremental query that loads host_activity_reduced

day-by-day
 */
insert into host_activity_reduced
with
-- using yesterday_array get the reduced data till yesterday
    yesterday_array as
        (
          select
                host,
                month,
                hit_array,
                unique_visitor_array
              from host_activity_reduced
              where month = '2023-01-01'
        ),
-- daily_agg will aggregate data on daily basis
    daily_agg as
         (
            select host,
                 cast(event_time :: timestamp as date) as date,
                 count(1)   as hit_cnt,
                 count(distinct user_id) as unique_visitor_cnt

            from events
            where cast(event_time :: timestamp as date) = (select (month +  (array_length(hit_array,1) ||'day')::interval) :: date from host_activity_reduced limit 1 )
                and user_id is not null
            group by host, cast(event_time :: timestamp as date)
        )
-- incremental query to load day by day
select coalesce(d.host, ya.host) as host,
       coalesce(date_trunc('month', d.date):: date,ya.month) as month,
       case
           when ya.host is not null
               then ya.hit_array || array[coalesce(d.hit_cnt, 0)]
           when ya.host is null
               then array_fill(0, array[(d.date - date_trunc('month', d.date):: date)]) || array [d.hit_cnt]
           end                                                as hit_array,
       case
           when ya.host is not null
               then ya.unique_visitor_array || array [coalesce(d.unique_visitor_cnt, 0)]
           when ya.host is null
               then array_fill(0, array [(d.date - date_trunc('month', d.date):: date)]) ||
                    array [d.unique_visitor_cnt]
           end                                                as unique_visitor_array
from daily_agg d
         full outer join yesterday_array ya
                         on d.host = ya.host
                             and date_trunc('month', d.date):: date = ya.month

on conflict (host,month)
do update
    set hit_array = excluded.hit_array,
        unique_visitor_array = excluded.unique_visitor_array
;

-- select host,month + ((nr-1)||'day')::interval ,month,host_activity_reduced.hit_array,elem,nr from host_activity_reduced
--                             cross join unnest(hit_array)with ordinality as a(elem,nr)
--     where host = 'www.eczachly.com' ;
--
-- select *  from host_activity_reduced;