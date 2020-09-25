--fdw/foreign table setup for nacc connection
CREATE EXTENSION postgres_fdw;

--fdw/foreign table setup for consentdb connection
CREATE SERVER consent_db_link FOREIGN DATA WRAPPER postgres_fdw OPTIONS (hostaddr '127.0.0.1', dbname '[DB_NAME]');

CREATE USER MAPPING FOR database_administrator
SERVER consent_db_link
OPTIONS (user 'database_administrator', password '[PASS]');

CREATE USER MAPPING FOR manager
SERVER consent_db_link[
OPTIONS (user 'manager', password '[PASS]');

CREATE USER MAPPING FOR viewer
SERVER consent_db_link
OPTIONS (user 'viewer', password '[PASS]');

CREATE FOREIGN TABLE adsp_lookup_linked
    (id integer,
    adsp_id text, 
    site_fam_id text, 
    site_indiv_id text, 
    cohort_identifier_code text, 
    lookup_id text, 
    adsp_family_id text, 
    adsp_indiv_partial_id text, 
    comments text, 
    subject_type text)

SERVER consentdb_link
OPTIONS (schema_name 'public', table_name 'adsp_lookup_linked');