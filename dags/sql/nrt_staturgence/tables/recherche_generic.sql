-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_generic table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_generic
(
    id_generic varchar(10) NOT NULL,
    nom        varchar(30),
    couleur    numeric(17),
    vigilance  varchar(4)
);