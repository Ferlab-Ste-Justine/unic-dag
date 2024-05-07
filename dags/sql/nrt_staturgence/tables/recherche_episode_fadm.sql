-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode_fadm table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode_fadm
(
    no_episode        decimal(9)   NOT NULL,
    no_patient        varchar(1)   NOT NULL,
    id_medicament     varchar(10),
    medicament        varchar(80),
    dose              varchar(20),
    voie              varchar(10),
    frequence         varchar(10),
    dh_administration timestamp(6) NOT NULL,
    dh_derniere_prise timestamp(6),
    dh_statut         timestamp(6)
);