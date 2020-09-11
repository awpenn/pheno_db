/*View definitions for phenotype database*/
--add--

/*Get all phenotype records, regardless of publish status*/
CREATE OR REPLACE VIEW get_all
AS
SELECT 
    subject_id,
    CAST(_data::json->>'sex' as INT) as sex,
    _data::json->>'prevad' as prevad,
    _data::json->>'incad' as incad,
    CAST(_data::json->>'age' as INT) as age,
    CAST(_data::json->>'age_baseline' as INT) as age_baseline,
    _data::json->>'apoe' as apoe,
    _data::json->>'autopsy' as autopsy,
    _data::json->>'braak' as braak,
    CAST(_data::json->>'race' as INT) as race,
    _data::json->>'ethnicity' as ethnicity,
    CAST(_data::json->>'selection' as INT) as selection,
    CAST(_data::json->>'ad' as INT) as ad,
    _data::json->>'comments' as comments,
    CAST(_data::json->>'data_version' as INT) as data_version,
    published
FROM ds_subjects_phenotypes
ORDER BY subject_id ASC, data_version DESC;

/*Get the most recent published dataset*/
CREATE OR REPLACE VIEW get_baseline
AS
SELECT 
   subject_id, 

CAST(_data::json->>'sex' as INT) as sex,
_data::json->>'prevad' as prevad,
_data::json->>'incad' as incad,
CAST(_data::json->>'age' as INT) as age,
CAST(_data::json->>'age_baseline' as INT) as age_baseline,
_data::json->>'apoe' as apoe,
_data::json->>'autopsy' as autopsy,
_data::json->>'braak' as braak,
CAST(_data::json->>'race' as INT) as race,
_data::json->>'ethnicity' as ethnicity,
CAST(_data::json->>'selection' as INT) as selection,
CAST(_data::json->>'ad' as INT) as ad,
_data::json->>'comments' as comments,
CAST(_data::json->>'data_version' as INT) as data_version
FROM ds_subjects_phenotypes
WHERE _data->>'data_version' IN (
        /*get the highest data_version id that's also published*/
        select distinct _data->>'data_version' as v from ds_subjects_phenotypes 
        WHERE published = TRUE
        ORDER BY v DESC LIMIT 1
    )
ORDER BY subject_id;

/*Get newest version, ie. the most recent data_version, regardless of publish status*/
CREATE OR REPLACE VIEW get_updated
AS
SELECT 
   subject_id, 

CAST(_data::json->>'sex' as INT) as sex,
_data::json->>'prevad' as prevad,
_data::json->>'incad' as incad,
CAST(_data::json->>'age' as INT) as age,
CAST(_data::json->>'age_baseline' as INT) as age_baseline,
_data::json->>'apoe' as apoe,
_data::json->>'autopsy' as autopsy,
_data::json->>'braak' as braak,
CAST(_data::json->>'race' as INT) as race,
_data::json->>'ethnicity' as ethnicity,
CAST(_data::json->>'selection' as INT) as selection,
CAST(_data::json->>'ad' as INT) as ad,
_data::json->>'comments' as comments,
CAST(_data::json->>'data_version' as INT) as data_version,
published
FROM ds_subjects_phenotypes
WHERE CAST(_data->>'data_version' as INT) IN (
        select id from data_versions where version_date = (select max(version_date) from data_versions)
    )
ORDER BY subject_id;


CREATE OR REPLACE VIEW get_current_dynamic
AS
SELECT 
subject_id, 
CAST(_data::json->>'sex' as INT) as sex,
_data::json->>'prevad' as prevad,
_data::json->>'incad' as incad,
CAST(_data::json->>'age' as INT) as age,
CAST(_data::json->>'age_baseline' as INT) as age_baseline,
_data::json->>'apoe' as apoe,
_data::json->>'autopsy' as autopsy,
_data::json->>'braak' as braak,
CAST(_data::json->>'race' as INT) as race,
_data::json->>'ethnicity' as ethnicity,
CAST(_data::json->>'selection' as INT) as selection,
CAST(_data::json->>'ad' as INT) as ad,
_data::json->>'comments' as comments,
CAST(_data::json->>'data_version' as INT) as data_version,
published

FROM get_current_published_dyno()
ORDER BY subject_id;

CREATE OR REPLACE VIEW get_newest_dynamic
AS
SELECT 
subject_id, 
CAST(_data::json->>'sex' as INT) as sex,
_data::json->>'prevad' as prevad,
_data::json->>'incad' as incad,
CAST(_data::json->>'age' as INT) as age,
CAST(_data::json->>'age_baseline' as INT) as age_baseline,
_data::json->>'apoe' as apoe,
_data::json->>'autopsy' as autopsy,
_data::json->>'braak' as braak,
CAST(_data::json->>'race' as INT) as race,
_data::json->>'ethnicity' as ethnicity,
CAST(_data::json->>'selection' as INT) as selection,
CAST(_data::json->>'ad' as INT) as ad,
_data::json->>'comments' as comments,
CAST(_data::json->>'data_version' as INT) as data_version,
published

FROM get_newest_dyno()
ORDER BY subject_id;