-- noinspection SqlNoDataSourceInspectionForFile

-- create sejour table
CREATE TABLE IF NOT EXISTS indicateurs_sip.sejour (
    lifetimenumber VARCHAR,
    encounterId Int PRIMARY KEY,
    admissionTypeOriginal VARCHAR,
    inTime Timestamp,
    dischargeDestination VARCHAR,
    outTime Timestamp,
    studyId Int,
    admissionType VARCHAR,
);