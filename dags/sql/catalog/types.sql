-- noinspection SqlNoDataSourceInspectionForFile

DO
$$
    BEGIN
        CREATE TYPE catalog.resource_type_enum AS ENUM ('warehouse', 'research_project', 'resource_project', 'eqp', 'source_system');
        CREATE TYPE catalog.value_type_enum AS ENUM ('integer', 'boolean', 'string', 'decimal', 'date', 'datetime');
        CREATE TYPE catalog.entity_type_enum AS ENUM ('patient', 'observation', 'diagnosis', 'medication', 'procedure', 'episode', 'encounter', 'delivery', 'pregnancy');
        CREATE TYPE catalog.domain_type_enum AS ENUM ('transfusion', 'imaging', 'medication', 'pathology', 'microbiology', 'laboratory', 'sociodemographics', 'diagnosis', 'pregnancy', 'medical_history');
        CREATE TYPE catalog.status_enum AS ENUM ('to_do','on_hold','in_progress','completed','delivered','removed');
        CREATE TYPE catalog.project_status_enum AS ENUM ('on hold', 'in review', 'in progress', 'delivered');
        CREATE TYPE catalog.project_active_enum AS ENUM ('completed', 'active');
        CREATE TYPE catalog.rolling_version_enum AS ENUM ('obsolete', 'current', 'future');
    EXCEPTION
        WHEN duplicate_object THEN null;
    END
$$;
