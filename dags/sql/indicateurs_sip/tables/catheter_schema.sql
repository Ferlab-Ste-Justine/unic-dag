-- noinspection SqlNoDataSourceInspectionForFile

-- create catheter table
CREATE TABLE IF NOT EXISTS indicateurs_sip.catheter (
    encounterId Int PRIMARY KEY,
    catheterType VARCHAR,
    catheterTime Timestamp);