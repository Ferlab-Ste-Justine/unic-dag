-- noinspection SqlNoDataSourceInspectionForFile

-- create recherche_locationsector table
CREATE TABLE IF NOT EXISTS nrt_staturgence.recherche_locationsector
(
    secteur     varchar(1) NOT NULL,
    urgence     varchar(2) NOT NULL,
    description varchar(30),
    type        varchar(1)
);