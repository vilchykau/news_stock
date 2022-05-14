create or replace procedure DWH.LINK_NEWS_URL()
    language PLPGSQL
as
$$
declare
begin
    insert into dwh.lne_originalstored_ur_at( ne_id_originalstored
                                            , ur_id_at
                                            , metadata_ne_originalstored_ur_at)
    select distinct nw.ne_id
                  , ur.ur_id
                  , 1
    from sa.news_company sn
             inner join dwh.lne_news nw on
            nw.ne_tit_news_title = sn.title
            and nw.ne_des_news_description = sn.description
             inner join dwh.lur_url ur on
        ur.ur_adr_url_address = sn.url
    where (nw.metadata_ne = 1)
      and not exists(select 1
                     from dwh.lne_originalstored_ur_at A
                     where A.ne_id_originalstored = nw.ne_id
                       and A.ur_id_at = ur.ur_id);

    insert into dwh.lne_imagestored_ur_at( ne_id_imagestored
                                         , ur_id_at
                                         , metadata_ne_imagestored_ur_at)
    select distinct nw.ne_id
                  , ur.ur_id
                  , 0
    from sa.news_company sn
             inner join dwh.lne_news nw on
            nw.ne_tit_news_title = sn.title
            and nw.ne_des_news_description = sn.description
             inner join dwh.lur_url ur on
        ur.ur_adr_url_address = sn.img_url
    where (nw.metadata_ne = 1)
    and not exists(select 1
                     from dwh.lne_imagestored_ur_at A
                     where A.ne_id_imagestored = nw.ne_id
                       and A.ur_id_at = ur.ur_id);
END;
$$;