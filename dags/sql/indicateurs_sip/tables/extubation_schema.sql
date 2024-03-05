-- noinspection SqlNoDataSourceInspectionForFile

-- create extubation table
CREATE TABLE IF NOT EXISTS indicateurs_sip.extubation (
    encounterId Int,
    nonPlannedExtubation VARCHAR,
    nonPlannedExtubationTime Timestamp);