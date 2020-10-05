/*View definitions for phenotype database*/
--add--

/*Get all cc phenotype records, regardless of publish status*/
CREATE OR REPLACE VIEW get_all_cc
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
        ds_subjects_phenotypes.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published
    FROM ds_subjects_phenotypes
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
    WHERE subject_type = 'case/control'
    ORDER BY subject_id ASC, data_version DESC;

/*Get PUBLISHED record with highest version number for a subject_id*/
CREATE OR REPLACE VIEW get_current_cc
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
        get_current_published_dyno.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published

    FROM get_current_published_dyno()
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
    WHERE subject_type = 'case/control'
    ORDER BY subject_id;

/*Get record with highest version number (published OR unpublished) for a subject_id*/
CREATE OR REPLACE VIEW get_newest_cc
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
        get_newest_dyno.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published

    FROM get_newest_dyno()
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
    WHERE subject_type = 'case/control'
    ORDER BY subject_id;

/*Return entries for subject_id with highest data_version number AND are unpublished*/
CREATE OR REPLACE VIEW get_unpublished_updates_cc
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
        get_updates_dyno.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published

    FROM get_updates_dyno()
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
    WHERE subject_type = 'case/control'
    ORDER BY subject_id;

/*WiP connect to consent db*/
-- CREATE OR REPLACE VIEW subjects_phenotypes_consents
--     AS
--     SELECT * FROM get_current_dynamic
--     LEFT JOIN adsp_lookup_linked
--     ON get_current_dynamic.subject_id = adsp_lookup_linked.adsp_id