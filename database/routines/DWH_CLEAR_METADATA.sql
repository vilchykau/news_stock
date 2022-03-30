create or replace procedure DWH.CLEAR_METADATA()
    language PLPGSQL
as
$$
declare
begin
    update DWH.co_company
    set metadata_co = 0
    where metadata_co = 1;

    update DWH.ne_news
    set metadata_ne = 0
    where metadata_ne = 1;

    update DWH.ur_url
    set metadata_ur = 0
    where metadata_ur = 1;
END;
$$;