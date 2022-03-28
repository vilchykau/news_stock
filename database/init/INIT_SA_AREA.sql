create table COMPANY
(
    NAME    varchar(100),
    QUERY   varchar(100),
    COUNTRY varchar(100)
);

alter table COMPANY
    owner to AIRFLOW;

create table NEWS_COMPANY
(
    COMPANY      varchar(500),
    SOURCE_NAME  varchar(500),
    AUTHOR       varchar(500),
    TITLE        varchar(500),
    DESCRIPTION  varchar(500),
    CONTENT      varchar(500),
    URL          varchar(500),
    IMG_URL      varchar(500),
    PUBLISH_DATE timestamp,
    INSERT_DT    timestamp default current_timestamp
);

alter table NEWS_COMPANY
    owner to AIRFLOW;

