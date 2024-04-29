-- noinspection SqlNoDataSourceInspectionForFile

-- create lits table
CREATE TABLE IF NOT EXISTS indicateurs_sip.lits (
    id Int,
    encounterId Int,
    bed Int,
    bedInTime Timestamp,
    bedOutTime Timestamp);