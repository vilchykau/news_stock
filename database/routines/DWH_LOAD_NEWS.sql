create or replace procedure DWH.LOAD_NEWS()
    language PLPGSQL
as
$$
declare
begin
    insert into DWH.LNE_NEWS(NE_TIT_NEWS_TITLE, NE_DES_NEWS_DESCRIPTION, METADATA_NE)
    select TITLE, DESCRIPTION, 1
    from SA.NEWS_COMPANY;
END;
$$;
