-- noinspection SqlNoDataSourceInspectionForFile

-- create extubation table
CREATE TABLE IF NOT EXISTS indicateurs_sip.extubation (
    encounterId Int PRIMARY KEY,
    nonPlannedExtubation VARCHAR,
    nonPlannedExtubationTime Timestamp);