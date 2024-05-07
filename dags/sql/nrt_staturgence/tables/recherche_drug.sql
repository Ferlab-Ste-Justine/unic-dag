-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_drug table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_drug
(
    no_medicament varchar(10) NOT NULL,
    marque        varchar(80),
    generique     varchar(10),
    vigilance     varchar(4),
    actif         decimal(5)
);