/*View definitions for phenotype database*/
/**case/control views**/
    /*Get all cc phenotype records, regardless of publish status*/
    CREATE OR REPLACE VIEW get_all_cc
        AS
        SELECT 
            subject_id,
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            CAST(_data::json->>'selection' as INT) as selection,
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

    /*Get PUBLISHED cc record with highest version number for a subject_id*/
    CREATE OR REPLACE VIEW get_current_cc
        AS
        SELECT 
        subject_id, 
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            CAST(_data::json->>'selection' as INT) as selection,
            _data::json->>'ad' as ad,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_current_published_cc_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM get_current_published_cc_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)

        ORDER BY subject_id ASC, data_version DESC;

    /*Get cc record with highest version number (published OR unpublished) for a subject_id*/
    CREATE OR REPLACE VIEW get_newest_cc
        AS
        SELECT 
            subject_id, 
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            CAST(_data::json->>'selection' as INT) as selection,
            _data::json->>'ad' as ad,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_newest_cc_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM get_newest_cc_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)

        ORDER BY subject_id ASC, data_version DESC;

    /*Return cc entries for subject_id with highest data_version number AND are unpublished*/
    CREATE OR REPLACE VIEW get_unpublished_updates_cc
        AS
        SELECT 
            subject_id, 
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            CAST(_data::json->>'selection' as INT) as selection,
            _data::json->>'ad' as ad,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_updates_cc_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM get_updates_cc_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)

        ORDER BY subject_id ASC, data_version DESC;
    /*baseline_cc*/
    CREATE OR REPLACE VIEW get_baseline_cc
        AS
        SELECT
            subject_id,
            CAST(_baseline_data->>'ad' as INT) as baseline_ad,
            _baseline_data->>'age' as baseline_age,
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
            _baseline_data->>'age_baseline' as baseline_age_baseline,
            CAST(_baseline_data->>'data_version' as INT) as baseline_data_version,
            data_versions.release_version as baseline_release_version
        FROM ds_subjects_phenotypes_baseline
        JOIN data_versions
            ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'data_version' AS INT)
        WHERE ds_subjects_phenotypes_baseline.subject_type = 'case/control'
        ORDER BY subject_id;

/**family views**/
    /*Get all fam phenotype records, regardless of publish status*/
    CREATE OR REPLACE VIEW get_all_fam
        AS
        SELECT 
            subject_id,
            _data::json->>'famid' as famid, 
            _data::json->>'mother' as mother, 
            _data::json->>'father' as father, 
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad' as ad,
            CAST(_data::json->>'famgrp' as INT) as famgrp,
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

    /*Get PUBLISHED fam record with highest version number for a subject_id*/
    CREATE OR REPLACE VIEW get_current_fam
        AS
        SELECT 
            subject_id,
            _data::json->>'famid' as famid, 
            _data::json->>'mother' as mother, 
            _data::json->>'father' as father, 
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad' as ad,
            CAST(_data::json->>'famgrp' as INT) as famgrp,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_current_published_family_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM get_current_published_family_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)

        ORDER BY subject_id ASC, data_version DESC;

    /*Get fam record with highest version number (published OR unpublished) for a subject_id*/
    CREATE OR REPLACE VIEW get_newest_fam
        AS
        SELECT 
            subject_id,
            _data::json->>'famid' as famid, 
            _data::json->>'mother' as mother, 
            _data::json->>'father' as father, 
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad' as ad,
            CAST(_data::json->>'famgrp' as INT) as famgrp,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_newest_family_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM get_newest_family_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)

        ORDER BY subject_id ASC, data_version DESC;

    /*Return fam entries for subject_id with highest data_version number AND are unpublished*/
    CREATE OR REPLACE VIEW get_unpublished_updates_fam
        AS
        SELECT 
            subject_id,
            _data::json->>'famid' as famid, 
            _data::json->>'mother' as mother, 
            _data::json->>'father' as father, 
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'age' as age,
            _data::json->>'age_baseline' as age_baseline,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            _data::json->>'ad' as ad,
            CAST(_data::json->>'famgrp' as INT) as famgrp,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_updates_family_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_adstatus' as BOOLEAN) as update_adstatus,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM get_updates_family_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)

        ORDER BY subject_id ASC, data_version DESC;
    /*baseline_fam*/
    CREATE OR REPLACE VIEW get_baseline_fam
        AS
        SELECT 
            subject_id,
        _baseline_data::json->>'famid' as baseline_famid,
        _baseline_data::json->>'mother' as baseline_mother,
        _baseline_data::json->>'father' as baseline_father,
        CAST(_baseline_data::json->>'sex' as INT) as baseline_sex,
        _baseline_data::json->>'age' as baseline_age,
        _baseline_data::json->>'apoe' as baseline_apoe,
        CAST(_baseline_data::json->>'race' as INT) as baseline_race,
        _baseline_data::json->>'braak' as baseline_braak,
        _baseline_data::json->>'autopsy' as baseline_autopsy,
        CAST(_baseline_data::json->>'ad' as INT) as baseline_ad,
        CAST(_baseline_data::json->>'famgrp' as INT) as baseline_famgrp,
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


/**ADNI views**/
    /*Get all adni phenotype records, regardless of publish status*/
    CREATE OR REPLACE VIEW get_all_adni
        AS
        SELECT 
            subject_id,
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age_current' as age_current,
            _data::json->>'age_baseline' as age_baseline,
            _data::JSON->>'age_mci_onset' as age_mci_onset,
            _data::JSON->>'age_ad_onset' as age_ad_onset,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            CAST(_data::json->>'ad_last_visit' as INT) as ad_last_visit,
            CAST(_data::json->>'mci_last_visit' as INT) as mci_last_visit,
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

    /*Get PUBLISHED adni record with highest version number for a subject_id*/
    CREATE OR REPLACE VIEW get_current_adni
        AS
        SELECT 
            subject_id,
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age_current' as age_current,
            _data::json->>'age_baseline' as age_baseline,
            _data::JSON->>'age_mci_onset' as age_mci_onset,
            _data::JSON->>'age_ad_onset' as age_ad_onset,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            CAST(_data::json->>'ad_last_visit' as INT) as ad_last_visit,
            CAST(_data::json->>'mci_last_visit' as INT) as mci_last_visit,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_current_published_adni_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction
            
        FROM get_current_published_adni_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'ADNI'
        ORDER BY subject_id ASC, data_version DESC;

    /*Get adni record with highest version number (published OR unpublished) for a subject_id*/
    CREATE OR REPLACE VIEW get_newest_adni
        AS
        SELECT 
            subject_id,
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age_current' as age_current,
            _data::json->>'age_baseline' as age_baseline,
            _data::JSON->>'age_mci_onset' as age_mci_onset,
            _data::JSON->>'age_ad_onset' as age_ad_onset,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            CAST(_data::json->>'ad_last_visit' as INT) as ad_last_visit,
            CAST(_data::json->>'mci_last_visit' as INT) as mci_last_visit,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_newest_adni_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction
            
        FROM get_newest_adni_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'ADNI'
        ORDER BY subject_id ASC, data_version DESC;
    /**/
    CREATE OR REPLACE VIEW get_unpublished_updates_adni
        AS
        SELECT 
            subject_id,
            CAST(_data::json->>'sex' as INT) as sex,
            _data::json->>'prevad' as prevad,
            _data::json->>'incad' as incad,
            _data::json->>'age_current' as age_current,
            _data::json->>'age_baseline' as age_baseline,
            _data::JSON->>'age_mci_onset' as age_mci_onset,
            _data::JSON->>'age_ad_onset' as age_ad_onset,
            _data::json->>'apoe' as apoe,
            _data::json->>'autopsy' as autopsy,
            _data::json->>'braak' as braak,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'ethnicity' as ethnicity,
            CAST(_data::json->>'ad_last_visit' as INT) as ad_last_visit,
            CAST(_data::json->>'mci_last_visit' as INT) as mci_last_visit,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_updates_adni_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction
            
        FROM get_updates_adni_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)
        WHERE subject_type = 'ADNI'
        ORDER BY subject_id ASC, data_version DESC;
    /*baseline data for adni records*/
    CREATE OR REPLACE VIEW get_baseline_adni
        AS
        SELECT
            subject_id,
            CAST(_baseline_data::json->>'sex' as INT) as baseline_sex,
            _baseline_data::json->>'apoe' as baseline_apoe,
            CAST(_baseline_data::json->>'race' as INT) as baseline_race,
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










/**these are probably going to be dropped**/
    /*Return current fam for a subject id with its baseline from the baseline table*/
    CREATE OR REPLACE VIEW get_current_and_baseline_cc
        AS 
        SELECT  
            get_current_cc.*, 
        CAST(_baseline_data->>'ad' as INT) as baseline_ad,
        _baseline_data->>'age' as baseline_age,
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

    /**/
    CREATE OR REPLACE VIEW get_current_and_baseline_fam
        AS 
        SELECT  
        get_current_fam.*, 
        _baseline_data::json->>'famid' as baseline_famid,
        _baseline_data::json->>'mother' as baseline_mother,
        _baseline_data::json->>'father' as baseline_father,
        _baseline_data::json->>'sex' as baseline_sex,
        _baseline_data::json->>'age' as baseline_age,
        _baseline_data::json->>'apoe' as baseline_apoe,
        CAST(_baseline_data::json->>'race' as INT) as baseline_race,
        _baseline_data::json->>'braak' as baseline_braak,
        _baseline_data::json->>'autopsy' as baseline_autopsy,
        CAST(_baseline_data::json->>'ad' as INT) as baseline_ad,
        CAST(_baseline_data::json->>'famgrp' as INT) as baseline_famgrp,
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

/**PSP/CDB views**/
    /*Get all psp/cdb phenotype records, regardless of publish status*/
    CREATE OR REPLACE VIEW get_all_psp_cdb
        AS
        SELECT 
            subject_id,
            CAST(_data::json->>'sex' as INT) as sex,
            CAST(_data::json->>'diagnosis' as INT) as diagnosis,
            _data::json->>'ageonset' as age_onset,
            _data::json->>'agedeath' as age_death,
            CAST(_data::json->>'race' as INT) as race,
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

    /*Get PUBLISHED psp/cdb record with highest version number for a subject_id*/
    CREATE OR REPLACE VIEW get_current_psp_cdb
        AS
        SELECT 
            subject_id,
            CAST(_data::json->>'sex' as INT) as sex,
            CAST(_data::json->>'diagnosis' as INT) as diagnosis,
            _data::json->>'ageonset' as age_onset,
            _data::json->>'agedeath' as age_death,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'duplicate_subjid' as duplicate_subjid,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_current_published_psp_cdb_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction


        FROM get_current_published_psp_cdb_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)

        ORDER BY subject_id ASC, data_version DESC;

    /*Get psp/cdb record with highest version number (published OR unpublished) for a subject_id*/
    CREATE OR REPLACE VIEW get_newest_psp_cdb
        AS
        SELECT 
            subject_id,
            CAST(_data::json->>'sex' as INT) as sex,
            CAST(_data::json->>'diagnosis' as INT) as diagnosis,
            _data::json->>'ageonset' as age_onset,
            _data::json->>'agedeath' as age_death,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'duplicate_subjid' as duplicate_subjid,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_newest_psp_cdb_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM get_newest_psp_cdb_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)

        ORDER BY subject_id ASC, data_version DESC;

    /*Return psp/cdb entries for subject_id with highest data_version number AND are unpublished*/
    CREATE OR REPLACE VIEW get_unpublished_updates_psp_cdb
        AS
        SELECT 
            subject_id,
            CAST(_data::json->>'sex' as INT) as sex,
            CAST(_data::json->>'diagnosis' as INT) as diagnosis,
            _data::json->>'ageonset' as age_onset,
            _data::json->>'agedeath' as age_death,
            CAST(_data::json->>'race' as INT) as race,
            _data::json->>'duplicate_subjid' as duplicate_subjid,
            _data::json->>'comments' as comments,
            CAST(_data::json->>'data_version' as INT) as data_version,
            get_updates_psp_cdb_dyno.published as record_published,
            d1.release_version as release_version,
            d1.published as version_published,
            CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
            CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
            CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
            CAST(_data::json->>'correction' as BOOLEAN) as correction

        FROM get_updates_psp_cdb_dyno()
        JOIN data_versions d1
            ON d1.id = CAST(_data::json->>'data_version' as INT)

        ORDER BY subject_id ASC, data_version DESC;
        
    /*baseline_psp_cdb*/
    CREATE OR REPLACE VIEW get_baseline_psp_cdb
        AS
        SELECT
            subject_id,
            CAST(_baseline_data::json->>'sex' as INT) as sex,
            CAST(_baseline_data::json->>'diagnosis' as INT) as diagnosis,
            _baseline_data::json->>'ageonset' as age_onset,
            _baseline_data::json->>'agedeath' as age_death,
            CAST(_baseline_data::json->>'race' as INT) as race,
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