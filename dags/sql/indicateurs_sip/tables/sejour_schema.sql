-- noinspection SqlNoDataSourceInspectionForFile

-- create sejour table
CREATE TABLE IF NOT EXISTS indicateurs_sip.sejour (
    encounterId Int PRIMARY KEY,
    lifetimenumber VARCHAR,
    admissionTypeOriginal VARCHAR,
    admissionType VARCHAR,
    dischargeDestination VARCHAR,
    inTime Timestamp,
    outTime Timestamp,
    studyId Int);