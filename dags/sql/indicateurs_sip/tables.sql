-- noinspection SqlNoDataSourceInspectionForFile

-- create catheter table
CREATE TABLE IF NOT EXISTS catheter (
    encounterId Int PRIMARY KEY,
    catheterType VARCHAR,
    catheterTime Timestamp);
-- create extubation table
CREATE TABLE IF NOT EXISTS extubation (
    encounterId Int PRIMARY KEY,
    nonPlannedExtubation VARCHAR,
    nonPlannedExtubationTime Timestamp);
-- create sejour table
CREATE TABLE IF NOT EXISTS sejour (
    encounterId Int PRIMARY KEY,
    admissionType VARCHAR,
    dischargeDestination VARCHAR,
    minInTime Timestamp,
    maxOutTime Timestamp,
    studyId VARCHAR);
-- create ventilation table
CREATE TABLE IF NOT EXISTS ventilation (
    encounterId Int PRIMARY KEY,
    invasiveVentilationDate Date);