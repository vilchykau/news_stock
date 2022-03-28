create procedure LOAD_NEWS()
    language PLPGSQL
as
$$
declare
begin
    insert into DBO.LNE_NEWS(NE_TIT_NEWS_TITLE, NE_DES_NEWS_DESCRIPTION, METADATA_NE)
    select TITLE, DESCRIPTION, 0
    from NEWS.NEWS_COMPANY;
END;
$$;

alter procedure LOAD_NEWS() owner to AIRFLOW;

