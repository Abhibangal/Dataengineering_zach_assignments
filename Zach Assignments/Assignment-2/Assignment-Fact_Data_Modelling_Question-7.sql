/*
A monthly, reduced fact table DDL host_activity_reduced

month
host
hit_array - think COUNT(1)
unique_visitors array - think COUNT(DISTINCT user_id)
 */
 --drop if already exists create table host_activity_reduced
 drop table if exists host_activity_reduced;
--DDL for create table host_activity_reduced
 create table host_activity_reduced
 (
    host text,
    month date,
    hit_array real[],
    unique_visitor_array real[],
    primary key (host,month)
 );

