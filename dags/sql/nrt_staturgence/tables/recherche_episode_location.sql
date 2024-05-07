-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode_location table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode_location
(
    no_episode      decimal(9) NOT NULL,
    no_sequence     decimal(5) NOT NULL,
    urgence         varchar(2) NOT NULL,
    dhm_debut       timestamp(6),
    no_location     varchar(6),
    dhm_fin         timestamp(6),
    dhm_suppression timestamp(6),
    civiere         decimal(5)
);