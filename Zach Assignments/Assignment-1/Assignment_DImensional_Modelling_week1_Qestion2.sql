/*
Cumulative table generation query: Write a query that populates the actors table one year at a time.
*/
--QUERY FOR CREATING A CUMULATIVE TABLE ACTORS
INSERT INTO actors
with get_last_year as (
    select coalesce(max(current_year),1969) as last_year
        from actors
    ),
    last_year as (
    select a.* from
                 (select
        actorid,
        actor,
        array_agg(row(
        film,
        votes,
        rating,
        filmid
        )::films_struct) as films,
        avg(rating) as rating,
        case when avg(rating) > 8 then 'star'
             when avg(rating) > 7 and avg(rating) <= 8 then 'good'
             when avg(rating) > 6 and avg(rating) <= 7 then 'good'
            else 'bad' end :: quality_class as quality_class,
        year
        from actor_films
        where year = (select last_year from get_last_year)
        group by    actorid,
                    actor,
                    year
        )  a
        cross join (select * from actor_films
                             where year = (select last_year + 1 from get_last_year)
                             limit 1)b
    ),
    current_year as (
            select
            actorid,
            actor,
            array_agg(row(
            film,
            votes,
            rating,
            filmid
            )::films_struct) as films,
            avg(rating) as rating,
            case when avg(rating) > 8 then 'star'
                 when avg(rating) > 7 and avg(rating) <= 8 then 'good'
                 when avg(rating) > 6 and avg(rating) <= 7 then 'good'
                else 'bad' end :: quality_class as quality_class,
            year
            from    actor_films
            where year = (select last_year + 1 from get_last_year)
            group by    actorid,
                        actor,
                        year
    )
select
    coalesce(c.actorid,l.actorid) as actorid,
    coalesce(c.actor,l.actor) as actor,
    case when c.films is not null then
            l.films || c.films
        when l.films is null then c.films
        else l.films end as films,
    coalesce(c.quality_class,l.quality_class)as quality_class,
    coalesce(c.year,l.year + 1)as current_year,
    case when c.actorid is not null then true
            else false end as is_active
from current_year c full outer join last_year l
    on c.actorid = l.actorid;

