-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode_transfert table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode_transfert
(
    no_episode      decimal(9)  NOT NULL,
    id_transfert    decimal(17) NOT NULL,
    urgence         varchar(2)  NOT NULL,
    dhm_demande     timestamp(6),
    dhm_accepte     timestamp(6),
    no_installation varchar(8),
    priorite        varchar(1),
    raison          varchar(2),
    dhm_suppression timestamp(6)
);