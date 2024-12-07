/*
A DDL for an user_devices_cumulated table that has:

a device_activity_datelist which tracks a users active days by browser_type
data type here should look similar to MAP<STRING, ARRAY[DATE]>
or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
*/
--drop if already exists user_devices_cumulated
drop table if exists user_devices_cumulated;

--DDL for user_devices_cumulated
create table user_devices_cumulated(
    user_id numeric,
    browser_type text,
    device_actives date[],
    date date,
    primary key (user_id,browser_type,date)
);
