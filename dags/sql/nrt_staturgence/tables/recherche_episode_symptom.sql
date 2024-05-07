-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode_symptom table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode_symptom
(
    no_episode  decimal(9)   NOT NULL,
    no_triage   decimal(5)   NOT NULL,
    urgence     varchar(2)   NOT NULL,
    dhm_symptom timestamp(6) NOT NULL,
    id_symptom  varchar(10)  NOT NULL,
    commentaire varchar(30)
);