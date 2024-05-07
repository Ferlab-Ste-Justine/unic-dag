-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_test table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_test
(
    famille     varchar(4) NOT NULL,
    examen      varchar(6) NOT NULL,
    urgence     varchar(2) NOT NULL,
    description varchar(25),
    detail      decimal(5),
    duree_max   decimal(5)
);