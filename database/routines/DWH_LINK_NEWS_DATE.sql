create or replace procedure DWH.link_news_date()
    language plpgsql
as
$$
declare
begin
    insert into DWH.lne_publish_da_when( metadata_ne_publish_da_when
                                       , ne_id_publish
                                       , da_id_when)
    select distinct 0
                  , N.ne_id
                  , D.da_id
    from SA.news_company S
             inner join DWH.lne_news N
                        on N.ne_tit_news_title = S.title
                            and N.ne_des_news_description = S.description
             inner join DWH.lda_date D
                        on D.da_dte_date_date = S.publish_date::date
    where N.metadata_ne = 1
      and not exists(select 1
                     from DWH.lne_publish_da_when A
                     where ne_id_publish = N.ne_id
                       and da_id_when = D.da_id);
END;
$$;
