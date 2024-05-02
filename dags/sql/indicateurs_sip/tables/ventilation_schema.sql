-- noinspection SqlNoDataSourceInspectionForFile

-- create ventilation table
CREATE TABLE IF NOT EXISTS indicateurs_sip.ventilation (
    encounterId Int,
    ventilationDateTime Timestamp,
    ventilationType VARCHAR);

