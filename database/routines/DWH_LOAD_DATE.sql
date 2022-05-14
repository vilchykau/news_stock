create or replace procedure DWH.load_date(SOURCE varchar(100))
    language plpgsql
as
$$
declare
    SOURCE_ID INT default -1;
begin
    SOURCE_ID := 0;

    if SOURCE = 'NEWS' then
        insert into DWH.lda_date( metadata_da
                                , metadata_da_day
                                , da_day_date_day
                                , metadata_da_mon
                                , da_mon_date_month
                                , metadata_da_yer
                                , da_yer_date_year
                                , metadata_da_dte
                                , da_dte_date_date)
        with cte_date as (
            select distinct EXTRACT(DAY FROM N.publish_date)   _DAY
                          , EXTRACT(MONTH FROM N.publish_date) _MONTH
                          , EXTRACT(YEAR FROM N.publish_date)  _YEAR
                          , N.publish_date::date               _DATE
            from SA.news_company N
        )
        select SOURCE_ID
             , SOURCE_ID
             , N._DAY
             , SOURCE_ID
             , N._MONTH
             , SOURCE_ID
             , N._YEAR
             , SOURCE_ID
             , N._DATE
        from cte_date N
        where not exists(select 1
                         from DWH.lda_date A
                         where A.da_dte_date_date = N._DATE);
    end if;
    if SOURCE = 'STOCKS' then
        insert into DWH.lda_date( metadata_da
                                , metadata_da_day
                                , da_day_date_day
                                , metadata_da_mon
                                , da_mon_date_month
                                , metadata_da_yer
                                , da_yer_date_year
                                , metadata_da_dte
                                , da_dte_date_date)
        with cte_date as (
            select distinct EXTRACT(DAY FROM N.trade_date)   _DAY
                          , EXTRACT(MONTH FROM N.trade_date) _MONTH
                          , EXTRACT(YEAR FROM N.trade_date)  _YEAR
                          , N.trade_date::date               _DATE
            from SA.stocks_company N
        )
        select SOURCE_ID
             , SOURCE_ID
             , N._DAY
             , SOURCE_ID
             , N._MONTH
             , SOURCE_ID
             , N._YEAR
             , SOURCE_ID
             , N._DATE
        from cte_date N
        where not exists(select 1
                         from DWH.lda_date A
                         where A.da_dte_date_date = N._DATE);
    end if;
END;
$$;
