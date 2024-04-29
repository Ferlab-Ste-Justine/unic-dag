-- noinspection SqlNoDataSourceInspectionForFile

-- create infirmiere table
CREATE TABLE IF NOT EXISTS indicateurs_sip.infirmiere (
    day Date,
    dayNurseCount Int,
    dayPatientCount Int,
    eveningNurseCount Int,
    eveningPatientCount Int,
    nightNurseCount Int,
    nightPatientCount Int);