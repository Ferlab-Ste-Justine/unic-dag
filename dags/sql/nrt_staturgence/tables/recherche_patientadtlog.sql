-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_patientadtlog table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_patientadtlog
(
    no_episode  decimal(9),
    no_dossier  varchar(10) NOT NULL,
    no_sequence decimal(5)  NOT NULL
);