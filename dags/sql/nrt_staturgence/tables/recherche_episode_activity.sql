-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode_activity table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode_activity
(
    no_episode   decimal(9)   NOT NULL,
    dhm_activite timestamp(6) NOT NULL,
    famille      varchar(10)  NOT NULL,
    id_activite  varchar(10)  NOT NULL,
    urgence      varchar(2)   NOT NULL,
    commentaire  varchar(30),
    utilisateur  varchar(8)
);