create or replace procedure DWH.LOAD_COMPANY()
    language PLPGSQL
as
$$
declare
begin
    insert into DWH.COT_COUNTRY (COT_COUNTRY, METADATA_COT)
    select distinct sac.country
                  , 0
    from SA.COMPANY sac
   	left join DWH.COT_COUNTRY dwh
   	    on dwh.cot_country = sac.country
   	where dwh.cot_country is null;
   
   	insert into dwh.lco_company 
	(co_nam_company_name, metadata_co)
	select distinct name, 1
	from SA.company
	left join dwh.lco_company 
		on co_nam_company_name = name
	where co_nam_company_name is null;

    insert into DWH.CO_COT_COMPANY_COUNTRY(CO_COT_CO_ID, CO_COT_COT_ID, METADATA_CO_COT)
    select comp.CO_ID,
           count.COT_ID,
           0
    from SA.COMPANY ncomp
    inner join DWH.LCO_COMPANY comp on comp.CO_NAM_COMPANY_NAME = ncomp.NAME
    inner join DWH.COT_COUNTRY count on ncomp.COUNTRY = count.COT_COUNTRY
   	where comp.metadata_co = 1;
END;
$$ ;