-- noinspection SqlNoDataSourceInspectionForFile

-- create infirmieres table
CREATE TABLE IF NOT EXISTS indicateurs_sip.infirmieres (
    date Date,
    encounterid Int,
    careproviderid VARCHAR,
    shift VARCHAR);