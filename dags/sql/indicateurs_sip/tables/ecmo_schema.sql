-- noinspection SqlNoDataSourceInspectionForFile

-- create ecmo table
CREATE TABLE IF NOT EXISTS indicateurs_sip.ecmo (
    encounterId Int,
    startEcmo Timestamp,
    endEcmo Timestamp,
    durationEcmoInDays Int
    );
