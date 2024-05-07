-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_activity table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_activity
(
    famille     varchar(10) NOT NULL,
    id_activite varchar(10) NOT NULL,
    urgence     varchar(2)  NOT NULL,
    description varchar(5),
    explication text,
    active      decimal(5)  NOT NULL
);