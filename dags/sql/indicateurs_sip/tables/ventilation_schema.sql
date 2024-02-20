-- noinspection SqlNoDataSourceInspectionForFile

-- create ventilation table
CREATE TABLE IF NOT EXISTS indicateurs_sip.ventilation (
    encounterId Int PRIMARY KEY,
    invasiveVentilationDate Date);