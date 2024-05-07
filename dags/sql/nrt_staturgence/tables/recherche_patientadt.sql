-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_patientadt table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_patientadt
(
    no_episode    decimal(9)  NOT NULL,
    no_dossier    varchar(10) NOT NULL,
    dhm_admission timestamp(6),
    dhm_depart    timestamp(6),
    medecin       varchar(10),
    specialite    varchar(4),
    diagnostic    varchar(60),
    unite         varchar(8)
);