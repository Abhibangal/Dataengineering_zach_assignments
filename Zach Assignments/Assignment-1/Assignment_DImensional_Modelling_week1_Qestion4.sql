/*
 Backfill query for actors_history_scd:
 Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
*/
insert into actors_history_scd
with changes as (select actorid,
                        actor,
                        actors.quality_class                                                     as current_quality_class,
                        lag(quality_class, 1) over (partition by actorid order by current_year ) as previous_quality_class,
                        is_active                                                                as current_status,
                        lag(is_active, 1) over (partition by actorid order by current_year )     as previous_status,
                        current_year,
                        lag(current_year, 1) over (partition by actorid order by current_year )  as previous_year
                 from actors),
 change_indi as (select
    actorid,
    actor,
    current_quality_class,
    current_status,
    case when current_quality_class <> previous_quality_class or current_status <> previous_status
        then 1
        else 0  end as change_indicator,
    current_year,
    previous_year
from changes),
    withstreak as(
select *,
       sum(change_indicator)over(partition by actorid order by current_year)as streak

from change_indi
)
select actorid,
        actor,
       current_quality_class,
       current_status AS is_active,
       min(current_year) as start_date,
       max(current_year) as end_date
from withstreak group by actorid,actor, streak,current_quality_class, current_status
order by actorid,actor,streak;

