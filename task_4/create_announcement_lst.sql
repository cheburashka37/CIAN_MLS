create table sschokorov.announcement_lst as 
    select pars.announcementid, pars.lat, pars.lng, pars.description, pars.address, pars.floornumber, pars.floorscount, 
            pars.category, pars.roomscount, pars.totalarea, pars.price, pars.pricetype, pars.status, 
            max(pars.dateinserted) as pars.dateinserted 
    from prod.announcement_lst as lst right join prod.announcement_parsed as pars 
        on pars.announcementid = lst.announcementid 
    group by pars.announcementid
        
        UNION ALL select lst.announcementid, lst.lat, lst.lng, lst.description, lst.address, lst.floornumber, lst.floorscount, 
            lst.category, lst.roomscount, lst.totalarea, lst.price, lst.pricetype, lst.status, lst.dateinserted 
    from prod.announcement_lst as lst left join prod.announcement_parsed as pars 
        on pars.announcementid = lst.announcementid where pars.announcementid is NULL;
