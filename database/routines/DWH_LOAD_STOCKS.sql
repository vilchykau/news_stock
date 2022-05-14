create or replace procedure DWH.load_stocks()
    language plpgsql
as
$$
declare
begin
    insert into dwh.ltr_trade( metadata_tr
                             , metadata_tr_opn
                             , tr_opn_trade_open
                             , metadata_tr_hgh
                             , tr_hgh_trade_high
                             , metadata_tr_low
                             , tr_low_trade_low
                             , metadata_tr_cls
                             , tr_cls_trade_close)
    select distinct 2
                  , 2
                  , S.open
                  , 2
                  , S.high
                  , 2
                  , S.low
                  , 2
                  , S.close
    from sa.stocks_company S
    where not exists(select 1
                     from dwh.ltr_trade A
                     where A.tr_opn_trade_open = S.open
                       and A.tr_hgh_trade_high = S.high
                       and A.tr_low_trade_low = S.low
                       and A.tr_cls_trade_close = S.close
        );
END;
$$;
