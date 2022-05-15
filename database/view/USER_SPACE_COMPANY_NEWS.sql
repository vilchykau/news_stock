drop view user_space.vw_company_news;

create view user_space.vw_company_news as
select ne.ne_tit_news_title as title,
       ne.ne_des_news_description as desc,
       co.co_nam_company_name as company_name,
       da.da_dte_date_date as date,
       ur.ur_adr_url_address image_url
from dwh.lne_news ne
left join dwh.co_company_ne_about ne_co_tie
    on ne.ne_id = ne_co_tie.ne_id_about
left join dwh.lco_company co
    on co.co_id = ne_co_tie.co_id_company
left join dwh.ne_imagestored_ur_at ne_imu
    on ne.ne_id = ne_imu.ne_id_imagestored
left join dwh.lur_url ur
    on ne_imu.ur_id_at = ur.ur_id
left join dwh.lne_publish_da_when npdw
    on npdw.ne_id_publish = ne.ne_id
left join dwh.lda_date da on npdw.da_id_when = da.da_id
order by da.da_dte_date_date desc;

SELECT * FROM user_space.vw_company_news limit 250
