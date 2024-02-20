-- noinspection SqlNoDataSourceInspectionForFile

-- create sejour table
CREATE TABLE IF NOT EXISTS indicateurs_sip.sejour (
    encounterId Int PRIMARY KEY,
    admissionType VARCHAR,
    dischargeDestination VARCHAR,
    minInTime Timestamp,
    maxOutTime Timestamp,
    studyId VARCHAR);