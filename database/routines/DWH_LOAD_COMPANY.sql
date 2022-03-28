create or replace procedure DWH.LOAD_COMPANY()
    language PLPGSQL
as
$$
declare
begin
    insert into DBO.COT_COUNTRY (COT_COUNTRY, METADATA_COT)
    select distinct COUNTRY, 0 from NEWS.COMPANY;

    insert into DBO.LCO_COMPANY(CO_NAM_COMPANY_NAME, METADATA_CO)
    select distinct NAME, 0 from NEWS.COMPANY;

    insert into DBO.CO_COT_COMPANY_COUNTRY(CO_COT_CO_ID, CO_COT_COT_ID, METADATA_CO_COT)
    select comp.CO_ID,
           count.COT_ID,
           0
    from SA.COMPANY ncomp
    inner join DBO.LCO_COMPANY comp on comp.CO_NAM_COMPANY_NAME = ncomp.NAME
    inner join DBO.COT_COUNTRY count on ncomp.COUNTRY = count.COT_COUNTRY;

    commit ;
END;
$$;

alter procedure DWH.LOAD_COMPANY() owner to AIRFLOW;