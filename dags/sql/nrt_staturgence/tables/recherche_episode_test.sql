-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode_test table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode_test
(
    no_episode  decimal(9)   NOT NULL,
    dhm_debut   timestamp(6) NOT NULL,
    famille     varchar(4)   NOT NULL,
    test        varchar(6),
    urgence     varchar(2)   NOT NULL,
    detail      varchar(6),
    commentaire varchar(30),
    dhm_statut  timestamp(6),
    statut      varchar(1)
);