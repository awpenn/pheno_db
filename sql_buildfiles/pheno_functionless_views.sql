-- DROPS 
    DROP VIEW _get_current_data CASCADE;
    DROP VIEW _get_unpublished_updates_data CASCADE;
    DROP VIEW _get_newest_data CASCADE;

    DROP VIEW cc_all CASCADE;
    DROP VIEW fam_all CASCADE;
    DROP VIEW adni_all CASCADE;
    DROP VIEW psp_cdb_all CASCADE;

    DROP VIEW cc_baseline CASCADE;
    DROP VIEW fam_baseline CASCADE;
    DROP VIEW adni_baseline CASCADE;
    DROP VIEW psp_cdb_baseline CASCADE;
--
--
--
-- JSON sorting views
---Get all current json
    CREATE OR REPLACE VIEW _get_current_data AS
        SELECT 
            ds_subjects_phenotypes.id, 
            ds_subjects_phenotypes.subject_id, 
            ds_subjects_phenotypes._data, 
            ds_subjects_phenotypes.subject_type, 
            ds_subjects_phenotypes.published  

        FROM
            (
            SELECT DISTINCT 
                ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') AS dv 
                FROM ds_subjects_phenotypes 
                JOIN data_versions
                    ON CAST(ds_subjects_phenotypes._data->>'data_version' AS INT) = data_versions.id
                WHERE ds_subjects_phenotypes.published = TRUE AND data_versions.published = TRUE 
                GROUP BY ds_subjects_phenotypes.subject_id
            ) as subq
        JOIN ds_subjects_phenotypes
        ON subq.subject_id = ds_subjects_phenotypes.subject_id AND subq.dv = ds_subjects_phenotypes._data->>'data_version';

---Get all unpublished updates json
    CREATE OR REPLACE VIEW _get_unpublished_updates_data AS
        SELECT 
            ds_subjects_phenotypes.id, 
            ds_subjects_phenotypes.subject_id, 
            ds_subjects_phenotypes._data, 
            ds_subjects_phenotypes.subject_type, 
            ds_subjects_phenotypes.published  

        FROM
            (
            SELECT DISTINCT 
                ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') AS dv 
                FROM ds_subjects_phenotypes 
                JOIN data_versions
                    ON CAST(ds_subjects_phenotypes._data->>'data_version' AS INT) = data_versions.id
                WHERE ds_subjects_phenotypes.published = FALSE AND data_versions.published = FALSE 
                GROUP BY ds_subjects_phenotypes.subject_id
            ) as subq
        JOIN ds_subjects_phenotypes
        ON subq.subject_id = ds_subjects_phenotypes.subject_id AND subq.dv = ds_subjects_phenotypes._data->>'data_version';
---Get newest json
    CREATE OR REPLACE VIEW _get_newest_data AS
        SELECT 
            ds_subjects_phenotypes.id, 
            ds_subjects_phenotypes.subject_id, 
            ds_subjects_phenotypes._data, 
            ds_subjects_phenotypes.subject_type, 
            ds_subjects_phenotypes.published  

        FROM
            (
            SELECT DISTINCT 
                ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') AS dv 
                FROM ds_subjects_phenotypes 
                JOIN data_versions
                    ON CAST(ds_subjects_phenotypes._data->>'data_version' AS INT) = data_versions.id
                GROUP BY ds_subjects_phenotypes.subject_id
            ) as subq
        JOIN ds_subjects_phenotypes
        ON subq.subject_id = ds_subjects_phenotypes.subject_id AND subq.dv = ds_subjects_phenotypes._data->>'data_version';

--
--
--
-- Case/control
---get all case/control
    CREATE OR REPLACE VIEW cc_all
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'selection' as selection,
            _data::json->>'ad' as ad,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            ds_subjects_phenotypes.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction
        FROM ds_subjects_phenotypes
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'case/control'
        ORDER BY subject_id ASC, data_version DESC;
---get current case/control
    CREATE OR REPLACE VIEW cc_current
        AS
        SELECT 
            subject_id, 
            _data::json->>'sex' as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'selection' as selection,
            _data::json->>'ad' as ad,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
           _get_current_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_current_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'case/control'
        ORDER BY subject_id ASC, data_version DESC;

---get unpublished updates case/control
    CREATE OR REPLACE VIEW cc_unpublished_updates
        AS
        SELECT 
            subject_id, 
            _data::json->>'sex' as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'selection' as selection,
            _data::json->>'ad' as ad,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
           _get_unpublished_updates_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_unpublished_updates_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'case/control'
        ORDER BY subject_id ASC, data_version DESC;


---get newest case/control
    CREATE OR REPLACE VIEW cc_newest
        AS
        SELECT 
            subject_id, 
            _data::json->>'sex' as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'selection' as selection,
            _data::json->>'ad' as ad,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_newest_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_newest_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'case/control'
        ORDER BY subject_id ASC, data_version DESC;
---get baseline case/control
    CREATE OR REPLACE VIEW cc_baseline
        AS
        SELECT
            subject_id,
            _baseline_data->>'ad' as baseline_ad,
            _baseline_data->>'age' as baseline_age,
            _baseline_data->>'sex' as baseline_sex,
            _baseline_data->>'apoe' as baseline_apoe,
            _baseline_data->>'race' as baseline_race,
            _baseline_data->>'braak' as baseline_braak,
            _baseline_data->>'incad' as baseline_incad,
            _baseline_data->>'prevad' as baseline_prevad,
            _baseline_data->>'autopsy' as baseline_autopsy,
            _baseline_data->>'comment' as baseline_comments,
            _baseline_data->>'ethnicity' as baseline_ethnicity,
            _baseline_data->>'selection' as baseline_selection,
            _baseline_data->>'age_baseline' as baseline_age_baseline,
            CAST(_baseline_data->>'data_version' as INT) as baseline_data_version,
            data_versions.release_version as baseline_release_version
        FROM ds_subjects_phenotypes_baseline
        JOIN data_versions
            ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'data_version' AS INT)
        WHERE ds_subjects_phenotypes_baseline.subject_type = 'case/control'
        ORDER BY subject_id;
---get latest published update case/control
    CREATE OR REPLACE VIEW cc_latest_published_version
        AS
        SELECT 
            subject_id, 
            _data::json->>'sex' as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'selection' as selection,
            _data::json->>'ad' as ad,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
           _get_current_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_current_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'case/control'
        AND _data->>'data_version' = ( SELECT DISTINCT MAX(_get_current_data._data->>'data_version') FROM _get_current_data WHERE subject_type = 'case/control')
        ORDER BY subject_id ASC, data_version DESC;

--
--
--
-- family
---get all family
    CREATE OR REPLACE VIEW fam_all
        AS
        SELECT 
            subject_id,
            _data::json->>'famid' as famid, 
            _data::json->>'mother' as mother, 
            _data::json->>'father' as father, 
            _data::json->>'sex' as sex,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad' as ad,
            _data::json->>'famgrp' as famgrp,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            ds_subjects_phenotypes.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction
        FROM ds_subjects_phenotypes
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'family'
        ORDER BY subject_id ASC, data_version DESC;
---get current family
    CREATE OR REPLACE VIEW fam_current
        AS
        SELECT 
            subject_id,
            _data::json->>'famid' as famid, 
            _data::json->>'mother' as mother, 
            _data::json->>'father' as father, 
            _data::json->>'sex' as sex,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad' as ad,
            _data::json->>'famgrp' as famgrp,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_current_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_current_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'family'
        ORDER BY subject_id ASC, data_version DESC;
---get unpublished updates family
    CREATE OR REPLACE VIEW fam_unpublished_updates
        AS
        SELECT 
            subject_id,
            _data::json->>'famid' as famid, 
            _data::json->>'mother' as mother, 
            _data::json->>'father' as father, 
            _data::json->>'sex' as sex,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad' as ad,
            _data::json->>'famgrp' as famgrp,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_unpublished_updates_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_unpublished_updates_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'family'
        ORDER BY subject_id ASC, data_version DESC;
---get newest family
    CREATE OR REPLACE VIEW fam_newest
        AS
        SELECT 
            subject_id,
            _data::json->>'famid' as famid, 
            _data::json->>'mother' as mother, 
            _data::json->>'father' as father, 
            _data::json->>'sex' as sex,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad' as ad,
            _data::json->>'famgrp' as famgrp,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_newest_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_newest_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'family'
        ORDER BY subject_id ASC, data_version DESC;
---get baseline family
    CREATE OR REPLACE VIEW fam_baseline
        AS
        SELECT 
            subject_id,
        _baseline_data::json->>'famid' as baseline_famid,
        _baseline_data::json->>'mother' as baseline_mother,
        _baseline_data::json->>'father' as baseline_father,
        _baseline_data::json->>'sex' as baseline_sex,
        _baseline_data::json->>'age' as baseline_age,
        _baseline_data::json->>'apoe' as baseline_apoe,
        _baseline_data::json->>'race' as baseline_race,
        _baseline_data::json->>'braak' as baseline_braak,
        _baseline_data::json->>'autopsy' as baseline_autopsy,
        _baseline_data::json->>'ad' as baseline_ad,
        _baseline_data::json->>'famgrp' as baseline_famgrp,
        _baseline_data::json->>'comment' as baseline_comments,
        _baseline_data::json->>'ethnicity' as baseline_ethnicity,
        _baseline_data::json->>'age_baseline' as baseline_age_baseline,
        CAST(_baseline_data::json->>'data_version' as INT) as baseline_data_version,
        data_versions.release_version as baseline_release_version       
        FROM ds_subjects_phenotypes_baseline 
        JOIN data_versions
            ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'data_version' AS INT)
        WHERE ds_subjects_phenotypes_baseline.subject_type = 'family'
        ORDER BY subject_id;
---get latest published update family
    CREATE OR REPLACE VIEW fam_latest_published_version
        AS
        SELECT 
            subject_id,
            _data::json->>'famid' as famid, 
            _data::json->>'mother' as mother, 
            _data::json->>'father' as father, 
            _data::json->>'sex' as sex,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad' as ad,
            _data::json->>'famgrp' as famgrp,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_current_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_current_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'family'
        AND _data->>'data_version' = ( SELECT DISTINCT MAX(_get_current_data._data->>'data_version') FROM _get_current_data WHERE subject_type = 'family')
        ORDER BY subject_id ASC, data_version DESC;
--
--
--
-- ADNI
---get all ADNI
    CREATE OR REPLACE VIEW adni_all
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age_current' as age_current,
            _data::json->>'age_baseline' as age_baseline,
            _data::JSON->>'age_mci_onset' as age_mci_onset,
            _data::JSON->>'age_ad_onset' as age_ad_onset,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad_last_visit' as ad_last_visit,
            _data::json->>'mci_last_visit' as mci_last_visit,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            ds_subjects_phenotypes.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM ds_subjects_phenotypes
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'ADNI'
        ORDER BY subject_id ASC, data_version DESC;
---get current ADNI
    CREATE OR REPLACE VIEW adni_current
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age_current' as age_current,
            _data::json->>'age_baseline' as age_baseline,
            _data::JSON->>'age_mci_onset' as age_mci_onset,
            _data::JSON->>'age_ad_onset' as age_ad_onset,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad_last_visit' as ad_last_visit,
            _data::json->>'mci_last_visit' as mci_last_visit,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_current_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction
            
        FROM _get_current_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'ADNI'
        ORDER BY subject_id ASC, data_version DESC;
---get unpublished updates ADNI
    CREATE OR REPLACE VIEW adni_unpublished_updates
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age_current' as age_current,
            _data::json->>'age_baseline' as age_baseline,
            _data::JSON->>'age_mci_onset' as age_mci_onset,
            _data::JSON->>'age_ad_onset' as age_ad_onset,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad_last_visit' as ad_last_visit,
            _data::json->>'mci_last_visit' as mci_last_visit,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_unpublished_updates_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction
            
        FROM _get_unpublished_updates_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'ADNI'
        ORDER BY subject_id ASC, data_version DESC;
---get newest ADNI
    CREATE OR REPLACE VIEW adni_newest
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age_current' as age_current,
            _data::json->>'age_baseline' as age_baseline,
            _data::JSON->>'age_mci_onset' as age_mci_onset,
            _data::JSON->>'age_ad_onset' as age_ad_onset,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad_last_visit' as ad_last_visit,
            _data::json->>'mci_last_visit' as mci_last_visit,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_newest_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction
            
        FROM _get_newest_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'ADNI'
        ORDER BY subject_id ASC, data_version DESC;
---get baseline ADNI
    CREATE OR REPLACE VIEW adni_baseline
        AS
        SELECT
            subject_id,
            _baseline_data::json->>'sex' as baseline_sex,
            _baseline_data::json->>'apoe' as baseline_apoe,
            _baseline_data::json->>'race' as baseline_race,
            _baseline_data::json->>'braak' as baseline_braak,
            _baseline_data::json->>'incad' as baseline_incad,
            _baseline_data::json->>'prevad' as baseline_prevad,
            _baseline_data::json->>'autopsy' as baseline_autopsy,
            _baseline_data::json->>'comments' as baseline_comments,
            _baseline_data::json->>'ethnicity' as baseline_ethnicity,
            _baseline_data::json->>'age_current' as baseline_age_current,
            _baseline_data::json->>'age_mci_onset' as baseline_age_mci_onset,
            _baseline_data::json->>'age_ad_onset' as baseline_age_ad_onset,
            _baseline_data::json->>'age_baseline' as baseline_age_baseline,
            CAST(_baseline_data::json->>'ad_last_visit' as INT) as baseline_ad_last_visit,
            CAST(_baseline_data::json->>'mci_last_visit' as INT) as baseline_mci_last_visit,
            CAST(_baseline_data::json->>'data_version' as INT) as baseline_data_version,
            data_versions.release_version as baseline_release_version     

        FROM ds_subjects_phenotypes_baseline 
        JOIN data_versions
            ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'data_version' AS INT)
        WHERE ds_subjects_phenotypes_baseline.subject_type = 'ADNI'
        ORDER BY subject_id;
---get latest published update ADNI
    CREATE OR REPLACE VIEW adni_latest_published_version
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age_current' as age_current,
            _data::json->>'age_baseline' as age_baseline,
            _data::JSON->>'age_mci_onset' as age_mci_onset,
            _data::JSON->>'age_ad_onset' as age_ad_onset,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            _data::json->>'race' as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad_last_visit' as ad_last_visit,
            _data::json->>'mci_last_visit' as mci_last_visit,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_current_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction
            
        FROM _get_current_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'ADNI'
        AND _data->>'data_version' = ( SELECT DISTINCT MAX(_get_current_data._data->>'data_version') FROM _get_current_data WHERE subject_type = 'ADNI')
        ORDER BY subject_id ASC, data_version DESC;
--
--
--
-- PSP/CDB
---get all PSP/CDB
    CREATE OR REPLACE VIEW psp_cdb_all
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'diagnosis' as diagnosis,
            _data::json->>'ageonset' as age_onset,
            _data::json->>'agedeath' as age_death,
            _data::json->>'race' as race,
            _data::json->>'duplicate_subjid' as duplicate_subjid,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            ds_subjects_phenotypes.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM ds_subjects_phenotypes
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'PSP/CDB'
        ORDER BY subject_id ASC, data_version DESC;
---get current PSP/CDB
    CREATE OR REPLACE VIEW psp_cdb_current
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'diagnosis' as diagnosis,
            _data::json->>'ageonset' as age_onset,
            _data::json->>'agedeath' as age_death,
            _data::json->>'race' as race,
            _data::json->>'duplicate_subjid' as duplicate_subjid,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_current_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_current_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'PSP/CDB'
        ORDER BY subject_id ASC, data_version DESC;
---get unpublished updates PSP/CDB
    CREATE OR REPLACE VIEW psp_cdb_unpublished_updates
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'diagnosis' as diagnosis,
            _data::json->>'ageonset' as age_onset,
            _data::json->>'agedeath' as age_death,
            _data::json->>'race' as race,
            _data::json->>'duplicate_subjid' as duplicate_subjid,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_unpublished_updates_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_unpublished_updates_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'PSP/CDB'
        ORDER BY subject_id ASC, data_version DESC;
---get newest PSP/CDB
    CREATE OR REPLACE VIEW psp_cdb_newest
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'diagnosis' as diagnosis,
            _data::json->>'ageonset' as age_onset,
            _data::json->>'agedeath' as age_death,
            _data::json->>'race' as race,
            _data::json->>'duplicate_subjid' as duplicate_subjid,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_newest_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_newest_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'PSP/CDB'
        ORDER BY subject_id ASC, data_version DESC;
---get baseline PSP/CDB
    CREATE OR REPLACE VIEW psp_cdb_baseline
        AS
        SELECT
            subject_id,
            _baseline_data::json->>'sex' as sex,
            _baseline_data::json->>'diagnosis' as diagnosis,
            _baseline_data::json->>'ageonset' as age_onset,
            _baseline_data::json->>'agedeath' as age_death,
            _baseline_data::json->>'race' as race,
            _baseline_data::json->>'duplicate_subjid' as duplicate_subjid,
            _baseline_data::json->>'comments' as comments,
            CAST(_baseline_data::json->>'data_version' as INT) as data_version,
            CAST(_baseline_data->>'data_version' as INT) as baseline_data_version,
            data_versions.release_version as baseline_release_version
        FROM ds_subjects_phenotypes_baseline
        JOIN data_versions
            ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'data_version' AS INT)
        WHERE ds_subjects_phenotypes_baseline.subject_type = 'PSP/CDB'
        ORDER BY subject_id;
---get latest published update PSP/CSB
    CREATE OR REPLACE VIEW psp_cdb_latest_published_version
        AS
        SELECT 
            subject_id,
            _data::json->>'sex' as sex,
            _data::json->>'diagnosis' as diagnosis,
            _data::json->>'ageonset' as age_onset,
            _data::json->>'agedeath' as age_death,
            _data::json->>'race' as race,
            _data::json->>'duplicate_subjid' as duplicate_subjid,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            _get_current_data.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM _get_current_data
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'PSP/CDB'
        AND _data->>'data_version' = ( SELECT DISTINCT MAX(_get_current_data._data->>'data_version') FROM _get_current_data WHERE subject_type = 'PSP/CDB')
        ORDER BY subject_id ASC, data_version DESC;
--
--
--