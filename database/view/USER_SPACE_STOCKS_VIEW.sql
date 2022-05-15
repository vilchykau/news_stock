drop view user_space.vw_stocks;

create view user_space.vw_stocks as
select tr.tr_opn_trade_open open
     , tr.tr_cls_trade_close "close"
     , tr.tr_hgh_trade_high hight
     , tr.tr_low_trade_low low
     , co.co_nam_company_name company_name
     , da.da_dte_date_date trade_date
from dwh.ltr_trade tr
left join dwh.ltr_trade_da_when ttdw on
    ttdw.tr_id_trade = tr.tr_id
left join dwh.lda_date da on
    da.da_id = ttdw.da_id_when
left join dwh.lco_off_tr_trade cott on
    cott.tr_id_trade = tr.tr_id
left join dwh.lco_company co on
    co.co_id = cott.co_id_off
order by trade_date;

select *
from user_space.vw_stocks;
