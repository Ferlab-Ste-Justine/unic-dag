-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode_consultation table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode_consultation
(
    no_episode         decimal(9) NOT NULL,
    id_consultation    decimal(5) NOT NULL,
    urgence            varchar(2) NOT NULL,
    dhm_demande        timestamp(6),
    demande_par        varchar(10),
    specialite         varchar(4),
    indicateur         varchar(1),
    dhm_realise        timestamp(6),
    realise_par        varchar(10),
    type_admission     varchar(2),
    raison_suppression varchar(4)
);