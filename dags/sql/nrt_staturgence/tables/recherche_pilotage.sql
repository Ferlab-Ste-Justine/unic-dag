-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_pilotage table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_pilotage
(
    type_table  varchar(28),
    urgence     varchar(2),
    identifiant varchar(30),
    description varchar(60),
    actif       decimal(5)
);