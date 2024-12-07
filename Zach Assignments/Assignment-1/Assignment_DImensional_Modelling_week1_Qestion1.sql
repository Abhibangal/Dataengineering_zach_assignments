/* Question1
DDL for actors table: Create a DDL for an actors table with the following fields:

films: An array of struct with the following fields:

film: The name of the film.
votes: The number of votes the film received.
rating: The rating of the film.
filmid: A unique identifier for each film.
quality_class: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:

star: Average rating > 8.
good: Average rating > 7 and ≤ 8.
average: Average rating > 6 and ≤ 7.
bad: Average rating ≤ 6.
*/
--creating a custom struct data type
create type films_struct as (
        film text,
        votes integer,
        rating real,
        filmid text
   );

--created quality class as enum type
create type quality_class as enum(
    'star',
    'good',
    'average',
    'bad');

--create TABLE actor
drop table  if exists actors;
create table actors(
    actorid text,
    actor text,
    films films_struct[],
    quality_class quality_class,
    current_year integer,
    is_active boolean,
    primary key (actorid,current_year)
);