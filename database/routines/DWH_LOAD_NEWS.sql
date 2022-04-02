create or replace procedure DWH.load_news()
    language plpgsql
as
$$
declare
begin
    insert into DWH.LNE_NEWS( NE_TIT_NEWS_TITLE
                            , NE_DES_NEWS_DESCRIPTION
                            , ne_pdt_news_publishdt
                            , METADATA_NE)
    select distinct
           TITLE
         , DESCRIPTION
         , publish_date
         , 1
    from SA.NEWS_COMPANY
             left join DWH.LNE_NEWS dwh
                       on title = dwh.ne_tit_news_title
                           and dwh.ne_des_news_description = description
                           and publish_date = ne_pdt_news_publishdt
    where dwh.ne_tit_news_title is null;
END;
$$;
