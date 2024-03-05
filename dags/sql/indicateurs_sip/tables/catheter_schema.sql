-- noinspection SqlNoDataSourceInspectionForFile

-- create catheter table
CREATE TABLE IF NOT EXISTS indicateurs_sip.catheter (
    encounterId Int,
    catheterType VARCHAR,
    catheterTime Timestamp);