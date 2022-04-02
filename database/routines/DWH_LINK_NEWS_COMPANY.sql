create or replace procedure DWH.LINK_NEWS_COMPANY()
    language PLPGSQL
as
$$
declare
begin
    insert into DWH.lco_company_ne_about( metadata_co_company_ne_about
                                        , co_id_company
                                        , ne_id_about)
    select distinct 0
                  , dwh_co.co_id
                  , dwh_ne.ne_id
    from sa.news_company sn
             inner join sa.company sc on sn.company = sc.query
             inner join DWH.lco_company dwh_co
                        on dwh_co.co_nam_company_name = sc.name
             inner join DWH.lne_news dwh_ne
                        on dwh_ne.ne_tit_news_title = sn.title
                            and dwh_ne.ne_des_news_description = sn.description
    where dwh_co.metadata_co = 1
       or dwh_ne.metadata_ne = 1;
END;
$$;