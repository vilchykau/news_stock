drop view user_space.vw_company_news;

create view user_space.vw_company_news as
select ne.ne_tit_news_title as title,
       ne.ne_des_news_description as,
       co.co_nam_company_name as company_name,
       ne.ne_pdt_news_publishdt,
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
order by ne.ne_pdt_news_publishdt desc;

SELECT * FROM user_space.vw_company_news limit 25
