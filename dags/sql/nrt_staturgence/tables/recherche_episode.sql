-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_episode table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_episode
(
    no_episode                   decimal(9)  NOT NULL,
    dhm_debut                    timestamp(6),
    dhm_depart                   timestamp(6),
    no_dossier                   varchar(10) NOT NULL,
    ddn_patient                  timestamp(6),
    adresse                      varchar(60),
    ville                        varchar(60),
    code_postal                  varchar(10),
    mode_arrivee                 varchar(2),
    type_orientation_depart      varchar(10),
    type_responsabilite_paiement varchar(2),
    urgence                      varchar(2)  NOT NULL,
    raison_consultation          varchar(50),
    autonomie_civiere            float8,
    provenance                   varchar(2),
    dhm_admit_real_annul         timestamp(6),
    dhm_statut                   timestamp(6),
    statut                       varchar(8),
    visite_active                decimal(5),
    dhm_creation                 timestamp(6),
    dhm_inscription              timestamp(6),
    no_accident                  varchar(4),
    dhm_accident                 timestamp(6),
    comment_accident             varchar(50),
    lieu_accident                varchar(4),
    dhm_debut_triage             timestamp(6),
    dhm_fin_triage               timestamp(6),
    comment_depart               varchar(255),
    dx_primaire                  varchar(10),
    dhm_deces                    timestamp(6),
    autopsie                     decimal(5),
    coroner                      decimal(5),
    dhm_pec                      timestamp(6),
    no_licence_pec               varchar(10),
    no_localisation              varchar(6),
    dhm_localisation             timestamp(6),
    dhm_civiere                  timestamp(6),
    consultation                 varchar(80),
    examen                       varchar(120),
    action_speciale              varchar(40),
    type_patient                 varchar(4),
    priorite_pretriage           varchar(1),
    code_priorite                varchar(1),
    dhm_demande_adm              timestamp(6)
);