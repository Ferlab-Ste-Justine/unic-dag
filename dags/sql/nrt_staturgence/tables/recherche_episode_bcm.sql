-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode_diagnostic table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode_diagnostic
(
    no_episode  decimal(9) NOT NULL,
    cim9        varchar(8) NOT NULL,
    urgence     varchar(2) NOT NULL,
    dhm_debut   timestamp(6),
    primaire    decimal(5),
    commentaire varchar(60)
);