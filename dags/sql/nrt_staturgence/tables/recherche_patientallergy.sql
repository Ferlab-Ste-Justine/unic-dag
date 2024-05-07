-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_patientallergy table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_patientallergy
(
    no_dossier   varchar(10) NOT NULL,
    allergie     varchar(10) NOT NULL,
    severite     varchar(2),
    reaction     varchar(4),
    dhm_allergie timestamp(6),
    commentaire  varchar(50)
);