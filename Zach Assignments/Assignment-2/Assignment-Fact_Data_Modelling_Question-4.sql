/*
 A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
 */
with date_series as
    -- generated date series for a month
        (
            select
                generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day')::date as series_date
        ),
    user_device_cum as
        (
           select u.user_id,
                  u.browser_type,
                  u.device_actives,
                  u.date ,
                  d.series_date from
            (
            select user_id,
                    browser_type,
                    device_actives,
                    date
             from user_devices_cumulated
             where date = '2023-01-14'

             )u cross join
            date_series d
        ),

    date_int as
         (
            select user_id,
                 browser_type,
                 device_actives,
                 case
                     when series_date = any (device_actives) then
                         pow(2, date_part('day',date) - 1 - (date - series_date)) :: bigint
                     else 0
                     end as date_int

            from user_device_cum
         )
select
    user_id,
    browser_type,
--     cast( sum(date_int)::bigint as bit(32))as datelist_bit, /*will be helpful for getting MAU,DAU,WAU*/
    sum(date_int)::bigint  as device_activity_datelist_int
    from date_int
group by user_id,
         browser_type
;


