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
        d1.release_version as release_version,
        d1.published as version_published,
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
        CAST(_data::json->>'correction' as BOOLEAN) as correction
    FROM ds_subjects_phenotypes
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)
    WHERE subject_type = 'case/control'
    ORDER BY subject_id ASC, data_version DESC;

/*Get PUBLISHED cc record with highest version number for a subject_id*/
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
        get_current_published_cc_dyno.published as record_published,
        d1.release_version as release_version,
        d1.published as version_published,
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
        CAST(_data::json->>'correction' as BOOLEAN) as correction

    FROM get_current_published_cc_dyno()
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)

    ORDER BY subject_id ASC, data_version DESC;

/*Get cc record with highest version number (published OR unpublished) for a subject_id*/
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
        get_newest_cc_dyno.published as record_published,
        d1.release_version as release_version,
        d1.published as version_published,
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
        CAST(_data::json->>'correction' as BOOLEAN) as correction

    FROM get_newest_cc_dyno()
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)

    ORDER BY subject_id ASC, data_version DESC;

/*Return cc entries for subject_id with highest data_version number AND are unpublished*/
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
        get_updates_cc_dyno.published as record_published,
        d1.release_version as release_version,
        d1.published as version_published,
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
        CAST(_data::json->>'correction' as BOOLEAN) as correction

    FROM get_updates_cc_dyno()
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)

    ORDER BY subject_id ASC, data_version DESC;

/*Get all cc phenotype records, regardless of publish status*/
CREATE OR REPLACE VIEW get_all_fam
    AS
    SELECT 
        subject_id,
        _data::json->>'family_id' as family_id, 
        _data::json->>'mother_id' as mother_id, 
        _data::json->>'father_id' as father_id, 
        CAST(_data::json->>'sex' as INT) as sex,
        CAST(_data::json->>'age' as INT) as age,
        CAST(_data::json->>'age_baseline' as INT) as age_baseline,
        _data::json->>'apoe' as apoe,
        _data::json->>'autopsy' as autopsy,
        _data::json->>'braak' as braak,
        CAST(_data::json->>'race' as INT) as race,
        _data::json->>'ethnicity' as ethnicity,
        CAST(_data::json->>'ad' as INT) as ad,
        CAST(_data::json->>'family_group' as INT) as family_group,
        _data::json->>'comments' as comments,
        CAST(_data::json->>'data_version' as INT) as data_version,
        ds_subjects_phenotypes.published as record_published,
        d1.release_version as release_version,
        d1.published as version_published,
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
        CAST(_data::json->>'correction' as BOOLEAN) as correction
    FROM ds_subjects_phenotypes
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)
    WHERE subject_type = 'family'
    ORDER BY subject_id ASC, data_version DESC;

/*Get PUBLISHED cc record with highest version number for a subject_id*/
CREATE OR REPLACE VIEW get_current_fam
    AS
    SELECT 
        subject_id,
        _data::json->>'family_id' as family_id, 
        _data::json->>'mother_id' as mother_id, 
        _data::json->>'father_id' as father_id, 
        CAST(_data::json->>'sex' as INT) as sex,
        CAST(_data::json->>'age' as INT) as age,
        CAST(_data::json->>'age_baseline' as INT) as age_baseline,
        _data::json->>'apoe' as apoe,
        _data::json->>'autopsy' as autopsy,
        _data::json->>'braak' as braak,
        CAST(_data::json->>'race' as INT) as race,
        _data::json->>'ethnicity' as ethnicity,
        CAST(_data::json->>'ad' as INT) as ad,
        CAST(_data::json->>'family_group' as INT) as family_group,
        _data::json->>'comments' as comments,
        CAST(_data::json->>'data_version' as INT) as data_version,
        get_current_published_family_dyno.published as record_published,
        d1.release_version as release_version,
        d1.published as version_published,
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
        CAST(_data::json->>'correction' as BOOLEAN) as correction

    FROM get_current_published_family_dyno()
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)

    ORDER BY subject_id ASC, data_version DESC;

/*Get cc record with highest version number (published OR unpublished) for a subject_id*/
CREATE OR REPLACE VIEW get_newest_fam
    AS
    SELECT 
        subject_id,
        _data::json->>'family_id' as family_id, 
        _data::json->>'mother_id' as mother_id, 
        _data::json->>'father_id' as father_id, 
        CAST(_data::json->>'sex' as INT) as sex,
        CAST(_data::json->>'age' as INT) as age,
        CAST(_data::json->>'age_baseline' as INT) as age_baseline,
        _data::json->>'apoe' as apoe,
        _data::json->>'autopsy' as autopsy,
        _data::json->>'braak' as braak,
        CAST(_data::json->>'race' as INT) as race,
        _data::json->>'ethnicity' as ethnicity,
        CAST(_data::json->>'ad' as INT) as ad,
        CAST(_data::json->>'family_group' as INT) as family_group,
        _data::json->>'comments' as comments,
        CAST(_data::json->>'data_version' as INT) as data_version,
        get_newest_family_dyno.published as record_published,
        d1.release_version as release_version,
        d1.published as version_published,
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
        CAST(_data::json->>'correction' as BOOLEAN) as correction

    FROM get_newest_family_dyno()
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)

    ORDER BY subject_id ASC, data_version DESC;

/*Return cc entries for subject_id with highest data_version number AND are unpublished*/
CREATE OR REPLACE VIEW get_unpublished_updates_fam
    AS
    SELECT 
        subject_id,
        _data::json->>'family_id' as family_id, 
        _data::json->>'mother_id' as mother_id, 
        _data::json->>'father_id' as father_id, 
        CAST(_data::json->>'sex' as INT) as sex,
        CAST(_data::json->>'age' as INT) as age,
        CAST(_data::json->>'age_baseline' as INT) as age_baseline,
        _data::json->>'apoe' as apoe,
        _data::json->>'autopsy' as autopsy,
        _data::json->>'braak' as braak,
        CAST(_data::json->>'race' as INT) as race,
        _data::json->>'ethnicity' as ethnicity,
        CAST(_data::json->>'ad' as INT) as ad,
        CAST(_data::json->>'family_group' as INT) as family_group,
        _data::json->>'comments' as comments,
        CAST(_data::json->>'data_version' as INT) as data_version,
        get_updates_family_dyno.published as record_published,
        d1.release_version as release_version,
        d1.published as version_published,
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
        CAST(_data::json->>'correction' as BOOLEAN) as correction

    FROM get_updates_family_dyno()
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)

    ORDER BY subject_id ASC, data_version DESC;
    

/*Return current cc for a subject id with its baseline from the baseline table*/
CREATE OR REPLACE VIEW get_current_and_baseline_cc
    AS 
    SELECT  
        get_current_cc.*, 
       CAST(_baseline_data->>'ad' as INT) as baseline_ad,
       CAST(_baseline_data->>'age' as INT) as baseline_age,
       _baseline_data->>'sex' as baseline_sex,
       _baseline_data->>'apoe' as baseline_apoe,
       CAST(_baseline_data->>'race' as INT) as baseline_race,
       _baseline_data->>'braak' as baseline_braak,
       _baseline_data->>'incad' as baseline_incad,
       _baseline_data->>'prevad' as baseline_prevad,
       _baseline_data->>'autopsy' as baseline_autopsy,
       _baseline_data->>'comment' as baseline_comments,
       _baseline_data->>'ethnicity' as baseline_ethnicity,
       CAST(_baseline_data->>'selection' as INT) as baseline_selection,
       _baseline_data->>'age_baseline' as baseline_age_baseline,
       CAST(_baseline_data->>'data_version' as INT) as baseline_data_version,
       data_versions.release_version as baseline_release_version
    FROM ds_subjects_phenotypes_baseline 
    JOIN get_current_cc
        ON ds_subjects_phenotypes_baseline.subject_id = get_current_cc.subject_id
    JOIN data_versions
        ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'data_version' AS INT)
    WHERE ds_subjects_phenotypes_baseline.subject_type = 'case/control'
    ORDER BY subject_id;

CREATE OR REPLACE VIEW get_current_and_baseline_fam
    AS 
    SELECT  
       get_current_fam.*, 
       _baseline_data::json->>'family_id' as baseline_family_id,
       _baseline_data::json->>'mother_id' as baseline_mother_id,
       _baseline_data::json->>'father_id' as baseline_father_id,
       _baseline_data::json->>'sex' as baseline_sex,
       CAST(_baseline_data::json->>'age' as INT) as baseline_age,
       _baseline_data::json->>'apoe' as baseline_apoe,
       CAST(_baseline_data::json->>'race' as INT) as baseline_race,
       _baseline_data::json->>'braak' as baseline_braak,
       _baseline_data::json->>'autopsy' as baseline_autopsy,
       CAST(_baseline_data::json->>'ad' as INT) as baseline_ad,
       CAST(_baseline_data::json->>'family_group' as INT) as baseline_family_group,
       _baseline_data::json->>'comment' as baseline_comments,
       _baseline_data::json->>'ethnicity' as baseline_ethnicity,
       _baseline_data::json->>'age_baseline' as baseline_age_baseline,
       CAST(_baseline_data::json->>'data_version' as INT) as baseline_data_version,
       data_versions.release_version as baseline_release_version       
    FROM ds_subjects_phenotypes_baseline 
    JOIN get_current_fam
        ON ds_subjects_phenotypes_baseline.subject_id = get_current_fam.subject_id
    JOIN data_versions
        ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'data_version' AS INT)
    WHERE ds_subjects_phenotypes_baseline.subject_type = 'family'
    ORDER BY subject_id;


CREATE OR REPLACE VIEW get_baseline_cc
    AS
    SELECT
        subject_id,
        CAST(_baseline_data->>'ad' as INT) as baseline_ad,
        CAST(_baseline_data->>'age' as INT) as baseline_age,
        CAST(_baseline_data->>'sex' as INT) as baseline_sex,
        _baseline_data->>'apoe' as baseline_apoe,
        CAST(_baseline_data->>'race' as INT) as baseline_race,
        _baseline_data->>'braak' as baseline_braak,
        _baseline_data->>'incad' as baseline_incad,
        _baseline_data->>'prevad' as baseline_prevad,
        _baseline_data->>'autopsy' as baseline_autopsy,
        _baseline_data->>'comment' as baseline_comments,
        _baseline_data->>'ethnicity' as baseline_ethnicity,
        CAST(_baseline_data->>'selection' as INT) as baseline_selection,
        CAST(_baseline_data->>'age_baseline' as INT) as baseline_age_baseline,
        CAST(_baseline_data->>'data_version' as INT) as baseline_data_version,
        data_versions.release_version as baseline_release_version
    FROM ds_subjects_phenotypes_baseline
    JOIN data_versions
        ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'data_version' AS INT)
    WHERE ds_subjects_phenotypes_baseline.subject_type = 'case/control'
    ORDER BY subject_id;

CREATE OR REPLACE VIEW get_baseline_fam
    AS
    SELECT 
        subject_id,
       _baseline_data::json->>'family_id' as baseline_family_id,
       _baseline_data::json->>'mother_id' as baseline_mother_id,
       _baseline_data::json->>'father_id' as baseline_father_id,
       CAST(_baseline_data::json->>'sex' as INT) as baseline_sex,
       CAST(_baseline_data::json->>'age' as INT) as baseline_age,
       _baseline_data::json->>'apoe' as baseline_apoe,
       CAST(_baseline_data::json->>'race' as INT) as baseline_race,
       _baseline_data::json->>'braak' as baseline_braak,
       _baseline_data::json->>'autopsy' as baseline_autopsy,
       CAST(_baseline_data::json->>'ad' as INT) as baseline_ad,
       CAST(_baseline_data::json->>'family_group' as INT) as baseline_family_group,
       _baseline_data::json->>'comment' as baseline_comments,
       _baseline_data::json->>'ethnicity' as baseline_ethnicity,
       CAST(_baseline_data::json->>'age_baseline' as INT) as baseline_age_baseline,
       CAST(_baseline_data::json->>'data_version' as INT) as baseline_data_version,
       data_versions.release_version as baseline_release_version       
    FROM ds_subjects_phenotypes_baseline 
    JOIN data_versions
        ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'data_version' AS INT)
    WHERE ds_subjects_phenotypes_baseline.subject_type = 'family'
    ORDER BY subject_id;
/*WiP connect to consent db*/
-- CREATE OR REPLACE VIEW subjects_phenotypes_consents
--     AS
--     SELECT * FROM get_current_dynamic
--     LEFT JOIN adsp_lookup_linked
--     ON get_current_dynamic.subject_id = adsp_lookup_linked.adsp_id





