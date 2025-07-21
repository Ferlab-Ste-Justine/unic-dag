-- noinspection SqlNoDataSourceInspectionForFile

DO
$$
    BEGIN
        CREATE TYPE catalog.resource_type_enum AS ENUM ('warehouse', 'research_project', 'eqp', 'source_system');
        CREATE TYPE catalog.value_type_enum AS ENUM ('integer', 'boolean', 'string', 'decimal', 'date', 'datetime', 'array', 'struct', 'map');
        CREATE TYPE catalog.entity_type_enum AS ENUM ('patient', 'observation', 'diagnosis', 'medication', 'procedure', 'episode', 'encounter', 'delivery', 'pregnancy', 'practitioner', 'location');
        CREATE TYPE catalog.domain_type_enum AS ENUM ('Administration', 'Adolescence', 'ADT (Admission, Discharge, and Transfer)',
        'Allergology', 'Anaesthesia', 'Anthropometry', 'Audiology', 'Cardiology', 'Cardiovascular Health', 'Complex Care',
        'Craniofacial', 'Cystic Fibrosis', 'Day Medicine', 'Dental Care', 'Dermatology', 'Diabetes', 'Diagnosis',
        'Emergency', 'Endocrinology', 'ENT (Ear, Nose, and Throat)', 'Ergotherapy', 'Fertility', 'Gastroenterology',
        'Genetics', 'Gynaecology', 'Hematology-Oncology', 'ICU (Intensive Care Unit)', 'Imaging', 'Immunology',
        'Infectious Diseases', 'Intervention', 'Laboratories', 'Medical History', 'Medication', 'Microbiology',
        'Neonatology', 'Nephrology', 'Neurodevelopment', 'Neurology', 'Neuropsychology', 'Neurosurgery', 'Nutrition',
        'Obstetrics', 'Oncology', 'Ophthalmology', 'Oral Medicine', 'Orthopedics', 'Pain', 'Palliative Care', 'Pathology',
        'Pediatrics', 'Physical Medicine', 'Physiotherapy', 'Plastic Surgery', 'Pneumology', 'Pregnancy', 'Psychiatry',
        'Psychology', 'Rehabilitation', 'Rheumatology', 'Sleep', 'Sociodemographics', 'Social Services', 'Speech Therapy',
        'Surgery', 'Telemedicine', 'Transfusion', 'Traumatology', 'Urology', 'Vital Signs');
        CREATE TYPE catalog.status_enum AS ENUM ('to_do','on_hold','in_progress','completed','delivered','removed');
        CREATE TYPE catalog.project_status_enum AS ENUM ('on_hold', 'in_review', 'in_progress', 'delivered', 'completed', 'reopened');
        CREATE TYPE catalog.rolling_version_enum AS ENUM ('obsolete', 'current', 'future');
    EXCEPTION
        WHEN duplicate_object THEN null;
    END
$$;
