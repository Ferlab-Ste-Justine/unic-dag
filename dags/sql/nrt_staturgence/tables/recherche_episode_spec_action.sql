-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode_spec_action table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode_spec_action
(
    no_episode decimal(9)  NOT NULL,
    id_action  varchar(10) NOT NULL,
    urgence    varchar(2)  NOT NULL,
    dhm_debut  timestamp(6)
);