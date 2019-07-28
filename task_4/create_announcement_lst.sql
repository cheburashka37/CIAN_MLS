
create table sschokorov.announcement_lst as 
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
        MAX(dateinserted) OVER(PARTITION BY announcementid) AS dateinserted 
    from (
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
        UNION ALL 
            select 
                *
            from prod.announcement_lst
    ) as u_;
