-- noinspection SqlNoDataSourceInspectionForFile

-- create sejour table
CREATE TABLE IF NOT EXISTS indicateurs_sip.sejour (
    studyId Int,
    encounterId Int,
    inTime Timestamp,
    dischargeDestination VARCHAR,
    outTime Timestamp,
    admissionType VARCHAR);