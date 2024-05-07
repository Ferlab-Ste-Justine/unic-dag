-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_location table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_location
(
    id_location   varchar(6) NOT NULL,
    secteur       varchar(1) NOT NULL,
    urgence       varchar(2) NOT NULL,
    categorie     varchar(1),
    active        decimal(5),
    particularite varchar(10)
);