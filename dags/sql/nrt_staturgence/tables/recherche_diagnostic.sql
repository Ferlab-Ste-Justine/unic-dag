-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_diagnostic table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_diagnostic
(
    secteur           varchar(10) NOT NULL,
    id_dx             varchar(10) NOT NULL,
    description       varchar(100),
    mot_cle           varchar(255),
    code_msss         varchar(10),
    indicateur_agence varchar(10),
    actif             decimal(5)
);