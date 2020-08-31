/*View definitions for phenotype database*/
DROP VIEW IF EXISTS get_current_cc;
DROP VIEW IF EXISTS get_current_ds;

CREATE VIEW get_current_cc AS
select version_date, subjects_phenotypes_cc.* 
FROM subjects_phenotypes_cc 
JOIN data_versions
ON subjects_phenotypes_cc.data_version = data_versions.id
WHERE data_version 
    IN (
        select id from data_versions where version_date = (select max(version_date) from data_versions
    )
)


CREATE OR REPLACE VIEW get_current_ds
AS
SELECT 
   subject_id, 

_data::json->'sex' as sex,
_data::json->'prevad' as prevAD,
_data::json->'incad' as incAD,
_data::json->'age' as age,
_data::json->'age_baseline' as age_baseline,
_data::json->'apoe' as apoe,
_data::json->'autopsy' as autopsy,
_data::json->'braak' as braak,
_data::json->'race' as race,
_data::json->'ethnicity' as ethnicity,
_data::json->'selection' as selection,
_data::json->'ad' as AD,
_data::json->'comments' as comments,
_data::json->'data_version' as data_version
FROM ds_subjects_phenotypes
WHERE CAST(_data->>'data_version' as INT) IN (
        select id from data_versions where version_date = (select max(version_date) from data_versions)
    );
