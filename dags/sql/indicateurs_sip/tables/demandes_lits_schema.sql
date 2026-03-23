-- noinspection SqlNoDataSourceInspectionForFile

-- create demandes_lits  table
CREATE TABLE IF NOT EXISTS indicateurs_sip.demandes_lits (
    encounterId Int,
    bedRequestedDateTime VARCHAR,
    bedReceivedDateTime VARCHAR);