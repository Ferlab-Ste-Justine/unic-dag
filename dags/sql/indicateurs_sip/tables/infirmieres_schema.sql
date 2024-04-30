-- noinspection SqlNoDataSourceInspectionForFile

-- create infirmieres table
CREATE TABLE IF NOT EXISTS indicateurs_sip.infirmieres (
    day Date,
    encounterid Int,
    careproviderid Int,
    shift Int);