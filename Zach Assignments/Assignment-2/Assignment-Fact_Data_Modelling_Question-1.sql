/*
A query to deduplicate game_details from Day 1 so there's no duplicates
 */
--step 1: Create temporary table to store distinct records for games_detail,devices and events tables
create  temp table if not exists tmp_games_detail
(
    game_id integer,
    team_id integer,
    team_abbreviation text,
    team_city text,
    player_id integer,
    player_name text,
    nickname text,
    start_position text,
    comment text,
    min text,
    fgm real,
    fga real,
    fg_pct real,
    fg3m real,
    fg3a real,
    fg3_pct real,
    ftm real,
    fta real,
    ft_pct real,
    oreb real,
    dreb real,
    reb real,
    ast real,
    stl real,
    blk real,
    "TO" real,
    pf real,
    pts real,
    plus_minus real
);
create temp table tmp_devices(
 device_id numeric,
  browser_type text,
  browser_version_major integer,
  browser_version_minor integer,
  browser_version_patch integer,
  device_type text,
  device_version_major text,
  device_version_minor integer,
  device_version_patch integer,
  os_type text,
  os_version_major text,
  os_version_minor integer,
  os_version_patch integer
);

create temp table tmp_events (
  url text,
  referrer text,
  user_id numeric,
  device_id numeric,
  host text,
  event_time text
);

--STEP 2: Query to distinct records from games_detail, devices and events tables and load respective temporary tables
-- games_detail
with dedupe_games_detail as (
    select gd.*,
            row_number() over (partition by gd.game_id,gd.player_id,gd.team_id) as row_num
        from game_details gd
        )
insert into tmp_games_detail
select
    game_id ,
    team_id ,
    team_abbreviation ,
    team_city ,
    player_id ,
    player_name ,
    nickname ,
    start_position ,
    comment ,
    min ,
    fgm ,
    fga ,
    fg_pct ,
    fg3m ,
    fg3a ,
    fg3_pct ,
    ftm ,
    fta ,
    ft_pct ,
    oreb ,
    dreb ,
    reb ,
    ast ,
    stl ,
    blk ,
    "TO" ,
    pf ,
    pts ,
    plus_minus
    from dedupe_games_detail
where row_num = 1;
--  events
with    dedupe_events as (
                select *,
                    row_number() over (partition by user_id,device_id,event_time) as row_num
                from events
                )

insert into tmp_events
select  url ,
        referrer ,
        user_id ,
        device_id ,
        host ,
        event_time
from dedupe_events where row_num = 1;

--devices
with       dedupe_devices as (
                select * ,
                    row_number() over (partition by device_id,browser_type)as row_num
                from devices
                )
insert into tmp_devices
select
        device_id ,
        browser_type ,
  browser_version_major ,
  browser_version_minor ,
  browser_version_patch ,
  device_type ,
  device_version_major ,
  device_version_minor ,
  device_version_patch ,
  os_type ,
  os_version_major ,
  os_version_minor ,
  os_version_patch
from dedupe_devices where row_num =1;


--STEP 3: Truncate games_details, devices and events tables
truncate table game_details;
truncate table devices;
truncate  table events;

-- STEP 4: Load devices and events table with distinct records from temporary tables
-- games_detail
insert into game_details
select * from tmp_games_detail;

--devices
insert into devices
select * from tmp_devices;

--events
insert into events
select * from tmp_events;
