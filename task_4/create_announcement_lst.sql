--set hive.exec.dynamic.partition.mode=nonstrict
--DROP TABLE IF EXISTS sschokorov.announcement_lst;
--CREATE TABLE sschokorov.announcement_lst (
--            announcementid bigint,
--            lat double, 
--            lng double, 
--            description string, 
--            address string, 
--            floornumber int, 
--            floorscount int, 
--            category string, 
--            roomscount int, 
--            totalarea double, 
--            price double, 
--            pricetype string, 
--            status int,
--            dateinserted timestamp
--) PARTITIONED BY (dadd date);

set day = to_date(CURRENT_TIMESTAMP);

WITH in_select_1 AS
(
    select 
            announcementid, 
            lat, 
            lng, 
            description, 
            address, 
            floornumber, 
            floorscount, 
            category, 
            roomscount, 
            totalarea, 
            price, 
            pricetype, 
            status, 
            dateinserted
        from prod.announcement_parsed
        where to_date(announcement_parsed.dateinserted) like DATE_ADD('2019-07-01', -1)
    UNION ALL 
        select 
            *
        from prod.announcement_lst
        --where to_date(announcement_lst.dateinserted) like DATE_ADD('2019-07-01', -2)
),

in_select_2 AS
(
    select 
        announcementid, 
        lat, 
        lng, 
        description, 
        address, 
        floornumber, 
        floorscount, 
        category, 
        roomscount, 
        totalarea, 
        price, 
        pricetype, 
        status, 
        RANK() OVER (PARTITION BY announcementid ORDER BY dateinserted) AS rank
    from in_select_1
)

INSERT OVERWRITE TABLE sschokorov.announcement_lst
PARTITION
(
    dadd = ${hiveconf:day}
)
SELECT *
FROM in_select_2
WHERE rank = 1;
