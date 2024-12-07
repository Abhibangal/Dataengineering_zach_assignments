/*
A DDL for hosts_cumulated table

a host_activity_datelist which logs to see which dates each host is experiencing any activity
 */
 --drop if already exists create table hosts_cumulated
 drop table if exists hosts_cumulated;
--DDL for create table hosts_cumulated
create table hosts_cumulated
(
    host text,
    host_activity_datelist date[],
    date date,
    primary key ( host ,date)
);
