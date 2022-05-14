create or replace procedure DWH.link_stocks_company()
    language plpgsql
as
$$
declare
begin
    delete
    from DWH._co_off_tr_trade
    where metadata_co_off_tr_trade = 2;

    insert into DWH.lco_off_tr_trade( metadata_co_off_tr_trade
                                    , co_id_off
                                    , tr_id_trade)
    select distinct 2
                  , C.co_id
                  , T.tr_id
    from SA.stocks_company S
             inner join DWH.ltr_trade T on S.open = T.tr_opn_trade_open
        and S.close = T.tr_cls_trade_close
        and S.high = T.tr_hgh_trade_high
        and S.low = T.tr_low_trade_low
             inner join DWH.lco_company C on upper(C.co_nam_company_name)
        = upper(S.company);
END;
$$;
