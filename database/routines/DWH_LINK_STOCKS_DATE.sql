create or replace procedure DWH.link_stocks_date()
    language plpgsql
as
$$
declare
begin
    insert into DWH.ltr_trade_da_when( metadata_tr_trade_da_when
                                     , tr_id_trade
                                     , da_id_when)
    select distinct 2
                  , T.tr_id
                  , D.da_id
    from SA.stocks_company S
             inner join DWH.ltr_trade T on S.open = T.tr_opn_trade_open
        and S.close = T.tr_cls_trade_close
        and S.high = T.tr_hgh_trade_high
        and S.low = T.tr_low_trade_low
             inner join DWH.lda_date D on D.da_dte_date_date = S.trade_date
    where not exists(select 1
                     from DWH.ltr_trade_da_when A
                     where A.tr_id_trade = T.tr_id
                       and A.da_id_when = D.da_id);
END;
$$;
