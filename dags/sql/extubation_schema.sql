-- noinspection SqlNoDataSourceInspectionForFile

-- create extubation table
CREATE TABLE IF NOT EXISTS extubation (
  encounterId Int PRIMARY KEY,
  nonPlannedExtubation VARCHAR,
  nonPlannedExtubationTime Timestamp);