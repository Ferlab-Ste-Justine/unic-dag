-- noinspection SqlNoDataSourceInspectionForFile

-- create resource table
CREATE TABLE IF NOT EXISTS catalog.resource
(
    id                              SERIAL PRIMARY KEY,
    last_update                     TIMESTAMP                  NOT NULL DEFAULT NOW(),
    code                            VARCHAR(20) UNIQUE         NOT NULL,
    name                            VARCHAR(255)               NOT NULL,
    title                           VARCHAR(255),
    resource_type                   catalog.resource_type_enum NOT NULL,
    description_en                  VARCHAR(1000)              NOT NULL,
    description_fr                  VARCHAR(1000)              NOT NULL,
    principal_investigator          VARCHAR(500),
    erb_project_id                  VARCHAR(255),
    project_creation_date           DATE,
    project_active                  catalog.project_active_enum,
    project_status                  catalog.project_status_enum,
    project_approved                BOOLEAN,
    project_folder                  VARCHAR(255),
    project_approval_date           DATE,
    project_completion_date         DATE,
    to_be_published                 BOOLEAN                    NOT NULL,
    system_database_type            VARCHAR(255),
    analyst_id                      INTEGER REFERENCES catalog.analyst (id),
    system_collection_starting_year INTEGER
);