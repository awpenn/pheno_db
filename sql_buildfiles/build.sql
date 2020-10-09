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
/*DYNO FUNCTIONS*/
    /*case/control*/
        /*get the entry with highest published vers_no for a particular cc subject_id*/
        CREATE OR REPLACE FUNCTION get_current_published_cc_dyno() RETURNS TABLE
        (id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
            declare
                i RECORD;
            BEGIN
                for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes WHERE ds_subjects_phenotypes.published = TRUE GROUP BY ds_subjects_phenotypes.subject_id)
                    loop
                        return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published FROM ds_subjects_phenotypes WHERE ds_subjects_phenotypes.subject_id = i.subject_id AND ds_subjects_phenotypes.subject_type = 'case/control' AND ds_subjects_phenotypes._data->>'data_version' = i.dv;
                    end loop;
            END;
        $$ LANGUAGE plpgsql;

        /*get the entry with highest vers_no for a particular cc subject_id, regardless of publication status*/
        CREATE OR REPLACE FUNCTION get_newest_cc_dyno() RETURNS TABLE
        (id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
            declare
                i RECORD;
            BEGIN
                for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv FROM ds_subjects_phenotypes GROUP BY ds_subjects_phenotypes.subject_id)
                    loop
                        return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published FROM ds_subjects_phenotypes WHERE ds_subjects_phenotypes.subject_id = i.subject_id AND ds_subjects_phenotypes.subject_type = 'case/control' AND ds_subjects_phenotypes._data->>'data_version' = i.dv;
                    end loop;
            END;
        $$ LANGUAGE plpgsql;

        /*Only return entries that for cc subject_id with highest data_version number AND are unpublished*/
        CREATE OR REPLACE FUNCTION get_updates_cc_dyno() RETURNS TABLE
        (id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
            declare
                i RECORD;
            BEGIN
                for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes GROUP BY ds_subjects_phenotypes.subject_id)
                    loop
                        return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published from ds_subjects_phenotypes WHERE ds_subjects_phenotypes.subject_id = i.subject_id AND ds_subjects_phenotypes._data->>'data_version' = i.dv AND ds_subjects_phenotypes.subject_type = 'case/control' AND ds_subjects_phenotypes.published = FALSE;
                    end loop;
            END;
        $$ LANGUAGE plpgsql;

    /*family*/
        /*get the entry with highest published vers_no for a particular cc subject_id*/
        CREATE OR REPLACE FUNCTION get_current_published_family_dyno() RETURNS TABLE
        (id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
            declare
                i RECORD;
            BEGIN
                for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes WHERE ds_subjects_phenotypes.published = TRUE GROUP BY ds_subjects_phenotypes.subject_id)
                    loop
                        return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published FROM ds_subjects_phenotypes WHERE ds_subjects_phenotypes.subject_id = i.subject_id AND ds_subjects_phenotypes.subject_type = 'family' AND ds_subjects_phenotypes._data->>'data_version' = i.dv;
                    end loop;
            END;
        $$ LANGUAGE plpgsql;

        /*get the entry with highest vers_no for a particular cc subject_id, regardless of publication status*/
        CREATE OR REPLACE FUNCTION get_newest_family_dyno() RETURNS TABLE
        (id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
            declare
                i RECORD;
            BEGIN
                for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv FROM ds_subjects_phenotypes GROUP BY ds_subjects_phenotypes.subject_id)
                    loop
                        return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published FROM ds_subjects_phenotypes WHERE ds_subjects_phenotypes.subject_id = i.subject_id AND ds_subjects_phenotypes.subject_type = 'family' AND ds_subjects_phenotypes._data->>'data_version' = i.dv;
                    end loop;
            END;
        $$ LANGUAGE plpgsql;

        /*Only return entries that for cc subject_id with highest data_version number AND are unpublished*/
        CREATE OR REPLACE FUNCTION get_updates_family_dyno() RETURNS TABLE
        (id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
            declare
                i RECORD;
            BEGIN
                for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes GROUP BY ds_subjects_phenotypes.subject_id)
                    loop
                        return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published from ds_subjects_phenotypes WHERE ds_subjects_phenotypes.subject_id = i.subject_id AND ds_subjects_phenotypes._data->>'data_version' = i.dv AND ds_subjects_phenotypes.subject_type = 'family' AND ds_subjects_phenotypes.published = FALSE;
                    end loop;
            END;
        $$ LANGUAGE plpgsql;
/*VIEWS*/
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
        CAST(_baseline_data->>'baseline_ad' as INT) as baseline_ad,
        CAST(_baseline_data->>'baseline_age' as INT) as baseline_age,
        _baseline_data->>'baseline_sex' as baseline_sex,
        _baseline_data->>'baseline_apoe' as baseline_apoe,
        CAST(_baseline_data->>'baseline_race' as INT) as baseline_race,
        _baseline_data->>'baseline_braak' as baseline_braak,
        _baseline_data->>'baseline_incad' as baseline_incad,
        _baseline_data->>'baseline_prevad' as baseline_prevad,
        _baseline_data->>'baseline_autopsy' as baseline_autopsy,
        _baseline_data->>'baseline_comment' as baseline_comment,
        _baseline_data->>'baseline_ethnicity' as baseline_ethnicity,
        CAST(_baseline_data->>'baseline_selection' as INT) as baseline_selection,
        _baseline_data->>'baseline_age_baseline' as baseline_age_baseline,
        CAST(_baseline_data->>'baseline_data_version' as INT) as baseline_data_version,
        data_versions.release_version as baseline_release_version
        FROM ds_subjects_phenotypes_baseline 
        JOIN get_current_cc
            ON ds_subjects_phenotypes_baseline.subject_id = get_current_cc.subject_id
        JOIN data_versions
            ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'baseline_data_version' AS INT)
        WHERE ds_subjects_phenotypes_baseline.subject_type = 'case/control'
        ORDER BY subject_id;

    CREATE OR REPLACE VIEW get_current_and_baseline_fam
        AS 
        SELECT  
        get_current_fam.*, 
        _baseline_data::json->>'baseline_family_id' as baseline_family_id,
        _baseline_data::json->>'baseline_mother_id' as baseline_mother_id,
        _baseline_data::json->>'baseline_father_id' as baseline_father_id,
        _baseline_data::json->>'baseline_sex' as baseline_sex,
        CAST(_baseline_data::json->>'baseline_age' as INT) as baseline_age,
        _baseline_data::json->>'baseline_apoe' as baseline_apoe,
        CAST(_baseline_data::json->>'baseline_race' as INT) as baseline_race,
        _baseline_data::json->>'baseline_braak' as baseline_braak,
        _baseline_data::json->>'baseline_autopsy' as baseline_autopsy,
        CAST(_baseline_data::json->>'baseline_ad' as INT) as baseline_ad,
        CAST(_baseline_data::json->>'family_group' as INT) as baseline_family_group,
        _baseline_data::json->>'baseline_comment' as baseline_comment,
        _baseline_data::json->>'baseline_ethnicity' as baseline_ethnicity,
        _baseline_data::json->>'baseline_age_baseline' as baseline_age_baseline,
        CAST(_baseline_data::json->>'baseline_data_version' as INT) as baseline_data_version,
        data_versions.release_version as baseline_release_version       
        FROM ds_subjects_phenotypes_baseline 
        JOIN get_current_fam
            ON ds_subjects_phenotypes_baseline.subject_id = get_current_fam.subject_id
        JOIN data_versions
            ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'baseline_data_version' AS INT)
        WHERE ds_subjects_phenotypes_baseline.subject_type = 'family'
        ORDER BY subject_id;

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
    CREATE TRIGGER subject_phenotypes_updated_at_modtime BEFORE UPDATE ON ds_subjects_phenotypes_baseline FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();


