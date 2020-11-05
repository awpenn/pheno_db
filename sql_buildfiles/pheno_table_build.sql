/*enum lists*/
CREATE TYPE "public"."subject_type" AS ENUM('case/control', 'family', 'other');   
/* Table definitions for phenotype database*/

CREATE TABLE IF NOT EXISTS "data_versions" 
    (
        "id" SERIAL NOT NULL, 
        "version_date" VARCHAR(25) NOT NULL, 
        "release_version" VARCHAR(50),
        "published" BOOLEAN DEFAULT FALSE NOT NULL, 
        "createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        "updatedat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, 
        PRIMARY KEY ("id")
    );

    /*as a demo for jsonb implementation*/
CREATE TABLE IF NOT EXISTS "ds_subjects_phenotypes"
    (
        "id" SERIAL NOT NULL,
        "subject_id" VARCHAR(50) NOT NULL,
        "_data" jsonb,
        "subject_type" "public"."subject_type" DEFAULT 'other' NOT NULL,
        "published" BOOLEAN DEFAULT FALSE NOT NULL,
        "createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        "updatedat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, 
        PRIMARY KEY ("id")
    );

CREATE TABLE IF NOT EXISTS "ds_subjects_phenotypes_baseline"
    (
        "id" SERIAL NOT NULL,
        "subject_id" VARCHAR(50) NOT NULL,
        "_baseline_data" jsonb,
        "subject_type" "public"."subject_type" DEFAULT 'other' NOT NULL,
        "createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        "updatedat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, 
        PRIMARY KEY ("id")
    );

CREATE TABLE IF NOT EXISTS "data_dictionaries"
    (
        "id" SERIAL NOT NULL,
        "dictionary_name" VARCHAR(50) NOT NULL,
        "_dict_data" jsonb,
        "createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        "updatedat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, 
        PRIMARY KEY ("id")
    );

/*this is original version of table with rows for each variable*/
-- CREATE TABLE IF NOT EXISTS "data_dictionaries"
--     (
--         "id" SERIAL NOT NULL,
--         "variable_name" VARCHAR(50) NOT NULL,
--         "dictionary_name" VARCHAR(50) NOT NULL,
--         "variable_description" VARCHAR(500),
--         "data_values" jsonb,
--         "comments" VARCHAR(500),
--         "createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
--         "updatedat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, 
--         PRIMARY KEY ("id")
--     );

-- /* example cc _data for subject_phenotypes table _data */
-- {
--     "ad": 0,
--     "age": 75,
--     "sex": 1,
--     "apoe": 34,
--     "race": 6,
--     "braak": "NA",
--     "incad": 0,
--     "prevad": 0,
--     "autopsy": 0,
--     "comment": "",
--     "ethnicity": 1,
--     "selection": 1,
--     "age_baseline": 60,
--     "data_version": 1,
--     "latest_update_version": 0,
--     "update_baseline": 0,
--     "update_latest": 0,
--     "update_adstatus": 0,
--     "correction": 0,
    }
-- 

-- /* example fam _data for subject_phenotypes table _data */
-- {
--     "family_id": 0,
--     "mother_id": 0,
--     "father_id": 0,
--     "sex": 1,
--     "age": 75,
--     "age_baseline": 60,
--     "apoe": 34,
--     "autopsy": 0,
--     "braak": "NA",
--     "race": 6,
--     "ethnicity": 1,
--     "family_group": 1,
--     "comment": "",
--     "data_version": 1,
--     "latest_update_version": 0,
--     "update_baseline,": 0,
--     "update_latest": 0,
--     "update_adstatus": 0,
--     "correction": 0
-- }

-- /* example cc _baseline_data for subject_phenotypes table _data */
-- {
--     "baseline_ad": 0,
--     "baseline_age": 75,
--     "baseline_sex": 1,
--     "baseline_apoe": 34,
--     "baseline_race": 6,
--     "baseline_braak": "NA",
--     "baseline_incad": 0,
--     "baseline_prevad": 0,
--     "baseline_autopsy": 0,
--     "baseline_comment": "",
--     "baseline_ethnicity": 1,
--     "baseline_selection": 1,
--     "baseline_age_baseline": 60,
--     "baseline_data_version": 1
    }
-- 

-- /* example fam _baseline_data for subject_phenotypes table _data */
-- {
--     "baseline_family_id": "0", 
--     "baseline_mother_id": "0", 
--     "baseline_father_id": "0", 
--     "baseline_sex": 1, 
--     "baseline_age": 75, 
--     "baseline_age_baseline": 60, 
--     "baseline_apoe": 34, 
--     "baseline_ad": 0,
--     "baseline_autopsy": 0, 
--     "baseline_braak": "NA", 
--     "baseline_race": 6, 
--     "baseline_ethnicity": 1, 
--     "baseline_family_group": 1,
--     "baseline_comment": "", 
--     "baseline_data_version": 1
-- }