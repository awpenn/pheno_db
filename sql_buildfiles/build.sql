/*TABLES*/
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

/*DYNO FUNCTIONS*/
    /*get the entry with highest published vers_no for a particular subject_id*/
    CREATE OR REPLACE FUNCTION get_current_published_dyno() RETURNS TABLE
    (id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
        declare
            i RECORD;
        BEGIN
            for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes where ds_subjects_phenotypes.published = TRUE GROUP BY ds_subjects_phenotypes.subject_id)
                loop
                    return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published from ds_subjects_phenotypes where ds_subjects_phenotypes.subject_id = i.subject_id and ds_subjects_phenotypes._data->>'data_version' = i.dv;
                end loop;
        END;
    $$ LANGUAGE plpgsql;

    /*get the entry with highest vers_no for a particular subject_id, regardless of publication status*/
    CREATE OR REPLACE FUNCTION get_newest_dyno() RETURNS TABLE
    (id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
        declare
            i RECORD;
        BEGIN
            for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes GROUP BY ds_subjects_phenotypes.subject_id)
                loop
                    return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published from ds_subjects_phenotypes where ds_subjects_phenotypes.subject_id = i.subject_id and ds_subjects_phenotypes._data->>'data_version' = i.dv;
                end loop;
        END;
    $$ LANGUAGE plpgsql;

    /*Only return entries that for subject_id with highest data_version number AND are unpublished*/
    CREATE OR REPLACE FUNCTION get_updates_dyno() RETURNS TABLE
    (id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
        declare
            i RECORD;
        BEGIN
            for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes GROUP BY ds_subjects_phenotypes.subject_id)
                loop
                    return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published from ds_subjects_phenotypes where ds_subjects_phenotypes.subject_id = i.subject_id and ds_subjects_phenotypes._data->>'data_version' = i.dv AND ds_subjects_phenotypes.published = FALSE;
                end loop;
        END;
    $$ LANGUAGE plpgsql;

/*VIEWS*/
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
        get_current_published_dyno.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published

    FROM get_current_published_dyno()
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
    WHERE subject_type = 'case/control'
    AND data_versions.published = TRUE
    ORDER BY subject_id;

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
        get_newest_dyno.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published

    FROM get_newest_dyno()
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
    WHERE subject_type = 'case/control'
    ORDER BY subject_id;

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
        get_updates_dyno.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published

    FROM get_updates_dyno()
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
    WHERE subject_type = 'case/control'
    ORDER BY subject_id;
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
        _data::json->>'family_group' as family_group,
        _data::json->>'comments' as comments,
        CAST(_data::json->>'data_version' as INT) as data_version,
        ds_subjects_phenotypes.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published
    FROM ds_subjects_phenotypes
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
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
        _data::json->>'family_group' as family_group,
        _data::json->>'comments' as comments,
        CAST(_data::json->>'data_version' as INT) as data_version,
        get_current_published_dyno.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published

    FROM get_current_published_dyno()
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
    WHERE subject_type = 'family'
    AND data_versions.published = TRUE
    ORDER BY subject_id;

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
        _data::json->>'family_group' as family_group,
        _data::json->>'comments' as comments,
        CAST(_data::json->>'data_version' as INT) as data_version,
        get_newest_dyno.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published

    FROM get_newest_dyno()
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
    WHERE subject_type = 'family'
    ORDER BY subject_id;

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
        _data::json->>'family_group' as family_group,
        _data::json->>'comments' as comments,
        CAST(_data::json->>'data_version' as INT) as data_version,
        get_updates_dyno.published as record_published,
        data_versions.release_version as release_version,
        data_versions.published as version_published

    FROM get_updates_dyno()
    JOIN data_versions
        ON data_versions.id = CAST(_data::json->>'data_version' as INT)
    WHERE subject_type = 'family'
    ORDER BY subject_id;


/*WiP connect to consent db*/
-- CREATE OR REPLACE VIEW subjects_phenotypes_consents
--     AS
--     SELECT * FROM get_current_dynamic
--     LEFT JOIN adsp_lookup_linked
--     ON get_current_dynamic.subject_id = adsp_lookup_linked.adsp_id
/*TRIGGERS*/
    -- updated at column trigger
    CREATE FUNCTION update_updated_at_column() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
    BEGIN
        NEW.updatedat = NOW();
        RETURN NEW;
    END;
    $$;

    CREATE TRIGGER subject_phenotypes_updated_at_modtime BEFORE UPDATE ON ds_subjects_phenotypes FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();


