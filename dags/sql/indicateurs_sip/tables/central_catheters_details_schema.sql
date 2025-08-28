-- noinspection SqlNoDataSourceInspectionForFile

-- create central_catheters_details  table
CREATE TABLE IF NOT EXISTS indicateurs_sip.central_catheters_details (
    encounterId Int,
    catheterType VARCHAR,
    catheterTime Timestamp,
    catheterSite VARCHAR,
    catheterCaliberAndNumberOfChannels VARCHAR);