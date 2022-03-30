drop view user_space.vw_company_news;

create view user_space.vw_company_news as
select ne.ne_tit_news_title as title,
       ne.ne_des_news_description as description,
       co.co_nam_company_name as company_name
from dwh.lne_news ne
left join dwh.co_company_ne_about ne_co_tie
    on ne.ne_id = ne_co_tie.ne_id_about
left join dwh.lco_company co
    on co.co_id = ne_co_tie.co_id_company;
