-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_logtransaction table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_logtransaction
(
    no_episode varchar(20)  NOT NULL,
    dh_statut  timestamp(6) NOT NULL,
    nom_table  varchar(15),
    urgence    varchar(2)   NOT NULL
);