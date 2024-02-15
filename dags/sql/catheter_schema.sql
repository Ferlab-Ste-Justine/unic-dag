-- noinspection SqlNoDataSourceInspectionForFile

-- create catheter table
CREATE TABLE IF NOT EXISTS catheter (
  encounterId Int PRIMARY KEY,
  catheterType VARCHAR,
  catheterTime Timestamp);