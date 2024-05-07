-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_quest_raison_visite table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_quest_raison_visite
(
    urgence      varchar(2)  NOT NULL,
    code_rv      varchar(10) NOT NULL,
    description  varchar(50),
    famille_rv   varchar(80),
    num_question float8,
    question     varchar(100),
    priorité     varchar(1)  NOT NULL,
    signe_vital  varchar(25)
);