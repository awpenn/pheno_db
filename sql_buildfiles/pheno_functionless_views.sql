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
    CREATE OR REPLACE VIEW get_all_cc
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
    CREATE OR REPLACE VIEW get_current_cc
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
    CREATE OR REPLACE VIEW get_unpublished_updates_cc
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
    CREATE OR REPLACE VIEW get_newest_cc
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
--
--
--
-- family
---get all family
    CREATE OR REPLACE VIEW get_all_fam
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
    CREATE OR REPLACE VIEW get_current_fam
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
    CREATE OR REPLACE VIEW get_unpublished_updates_fam
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
    CREATE OR REPLACE VIEW get_unpublished_updates_fam
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
--
--
--
-- ADNI
---get all ADNI
    CREATE OR REPLACE VIEW get_all_adni
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
    CREATE OR REPLACE VIEW get_current_adni
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
    CREATE OR REPLACE VIEW get_unpublished_updates_adni
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
    CREATE OR REPLACE VIEW get_newest_adni
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
--
--
--
-- PSP/CDB
---get all PSP/CDB
    CREATE OR REPLACE VIEW get_all_psp_cdb
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
    CREATE OR REPLACE VIEW get_current_psp_cdb
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
    CREATE OR REPLACE VIEW get_unpublished_updates_cdb
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
    CREATE OR REPLACE VIEW get_newest_cdb
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