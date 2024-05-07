-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode_pec table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode_pec
(
    no_episode    decimal(9)  NOT NULL,
    id_pec        decimal(5)  NOT NULL,
    dhm_pec       timestamp(6),
    no_permis_md  varchar(10),
    id_specialite varchar(4),
    no_dossier    varchar(10) NOT NULL
);