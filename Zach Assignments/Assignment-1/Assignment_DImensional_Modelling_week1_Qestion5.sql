/*
Incremental query for actors_history_scd:
Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.
*/
    with
--  historical_scd as (
--             select  actorid,
--                     actor,
--                     quality_class,
--                     is_active,
--                     start_date,
--                     end_date
--             from actors_history_scd where end_date < (select max(end_date) - 1 from actors_history_scd)
--     ),
    last_year_data as (
            select  actorid,
                    actor,
                    quality_class,
                    is_active,
                    start_date,
                    end_date
            from actors_history_scd where end_date = (select max(end_date)  from actors_history_scd)
    ),
    current_year_data as (
            select  actorid,
                    actor,
                    quality_class,
                    is_active,
                    actors.current_year as start_date,
                    actors.current_year  as end_date
            from actors where current_year = (select max(current_year) from actors)
    ),
    matched_data as (
        select  c.actorid,
                c.actor,
                c.quality_class,
                c.is_active,
                l.start_date,
                c.end_date
        from current_year_data c
         join last_year_data l
        on l.actorid = c.actorid
        where c.quality_class = l.quality_class and
               c.is_active = l.is_active

    ),
    updated_new_data as (
        select  c.actorid,
                c.actor,
                c.quality_class,
                c.is_active,
                c.start_date,
                c.end_date
        from current_year_data c
        left join last_year_data l
        on l.actorid = c.actorid
        where (c.quality_class <> l.quality_class or
               c.is_active <> l.is_active)
        or l.actorid is null

    ),
    union_all_data as (
                select  m.actorid,
                m.actor,
                m.quality_class,
                m.is_active,
                m.start_date,
                m.end_date
        from matched_data m
        union all
         select  c.actorid,
                c.actor,
                c.quality_class,
                c.is_active,
                c.start_date,
                c.end_date
        from updated_new_data c

    )
insert into  actors_history_scd
  (actorid, actor, quality_class, is_active, start_date, end_date)
 select actorid,actor,quality_class,is_active,start_date,end_date  from union_all_data
  on conflict (actorid,start_date)
    do update
       set quality_class = excluded.quality_class,
           is_active = excluded.is_active,
           end_date = excluded.end_date;









