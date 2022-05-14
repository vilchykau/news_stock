create or replace procedure DWH.LOAD_URL()
    language PLPGSQL
as
$$
declare
begin
    insert into DWH.utp_urltype(utp_urltype, metadata_utp)
    select distinct url_type.url_type, 0
    from sa.url_type
    left join dwh.utp_urltype on url_type.url_type = utp_urltype
    where utp_urltype is null;

    insert into DWH.lur_url(ur_adr_url_address, ur_typ_utp_id, metadata_ur)
    select distinct sat.url
         , dw.utp_id
         , 0
    from sa.news_company sat
    inner join DWH.utp_urltype dw on 'NEWS' = dw.utp_urltype
    left join DWH.lur_url old on old.ur_adr_url_address = sat.url
    where old.ur_adr_url_address is null;

    insert into DWH.lur_url(ur_adr_url_address, ur_typ_utp_id, metadata_ur)
    select distinct sat.img_url
         , dw.utp_id
         , 0
    from sa.news_company sat
    inner join DWH.utp_urltype dw on 'IMAGE' = dw.utp_urltype
    left join DWH.lur_url old on old.ur_adr_url_address = sat.img_url
    where old.ur_adr_url_address is null;
END;
$$;