/*psp/cdb*/
	/*get the entry with highest published vers_no for a particular cc subject_id*/
	CREATE OR REPLACE FUNCTION get_current_published_psp_cdb_dyno() RETURNS TABLE
	(id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
		declare
			i RECORD;
		BEGIN
			for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes WHERE ds_subjects_phenotypes.published = TRUE GROUP BY ds_subjects_phenotypes.subject_id)
				loop
					return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published FROM ds_subjects_phenotypes WHERE ds_subjects_phenotypes.subject_id = i.subject_id AND ds_subjects_phenotypes.subject_type = 'PSP/CDB' AND ds_subjects_phenotypes._data->>'data_version' = i.dv;
				end loop;
		END;
	$$ LANGUAGE plpgsql;

	/*get the entry with highest vers_no for a particular cc subject_id, regardless of publication status*/
	CREATE OR REPLACE FUNCTION get_newest_psp_cdb_dyno() RETURNS TABLE
	(id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
		declare
			i RECORD;
		BEGIN
			for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv FROM ds_subjects_phenotypes GROUP BY ds_subjects_phenotypes.subject_id)
				loop
					return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published FROM ds_subjects_phenotypes WHERE ds_subjects_phenotypes.subject_id = i.subject_id AND ds_subjects_phenotypes.subject_type = 'PSP/CDB' AND ds_subjects_phenotypes._data->>'data_version' = i.dv;
				end loop;
		END;
	$$ LANGUAGE plpgsql;

	/*Only return entries that for cc subject_id with highest data_version number AND are unpublished*/
	CREATE OR REPLACE FUNCTION get_updates_psp_cdb_dyno() RETURNS TABLE
	(id int, subject_id char varying, _data jsonb, subject_type subject_type, published boolean) AS $$
		declare
			i RECORD;
		BEGIN
			for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes GROUP BY ds_subjects_phenotypes.subject_id)
				loop
					return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.subject_type, ds_subjects_phenotypes.published from ds_subjects_phenotypes WHERE ds_subjects_phenotypes.subject_id = i.subject_id AND ds_subjects_phenotypes._data->>'data_version' = i.dv AND ds_subjects_phenotypes.subject_type = 'PSP/CDB' AND ds_subjects_phenotypes.published = FALSE;
				end loop;
		END;
	$$ LANGUAGE plpgsql;



    /**PSP/CDB views**/
/*Get all cc phenotype records, regardless of publish status*/
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
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
        CAST(_data::json->>'correction' as BOOLEAN) as correction

    FROM ds_subjects_phenotypes
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)
    WHERE subject_type = 'PSP/CDB'
    ORDER BY subject_id ASC, data_version DESC;

/*Get PUBLISHED cc record with highest version number for a subject_id*/
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
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
        CAST(_data::json->>'correction' as BOOLEAN) as correction


    FROM get_current_published_psp_cdb_dyno()
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)

    ORDER BY subject_id ASC, data_version DESC;

/*Get cc record with highest version number (published OR unpublished) for a subject_id*/
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
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
        CAST(_data::json->>'correction' as BOOLEAN) as correction

    FROM get_newest_psp_cdb_dyno()
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)

    ORDER BY subject_id ASC, data_version DESC;

/*Return cc entries for subject_id with highest data_version number AND are unpublished*/
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
        d2.release_version as latest_update_version,
        CAST(_data::json->>'update_baseline' as BOOLEAN) as update_baseline,
        CAST(_data::json->>'update_latest' as BOOLEAN) as update_latest,
        CAST(_data::json->>'update_diagnosis' as BOOLEAN) as update_diagnosis,
        CAST(_data::json->>'correction' as BOOLEAN) as correction

    FROM get_updates_psp_cdb_dyno()
    JOIN data_versions d1
        ON d1.id = CAST(_data::json->>'data_version' as INT)
	JOIN data_versions d2
		ON d2.id = CAST(_data::json->>'latest_update_version' as INT)

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
        CAST(_baseline_data->>'selection' as INT) as baseline_selection,
        _baseline_data->>'age_baseline' as baseline_age_baseline,
        CAST(_baseline_data->>'data_version' as INT) as baseline_data_version,
        data_versions.release_version as baseline_release_version
    FROM ds_subjects_phenotypes_baseline
    JOIN data_versions
        ON data_versions.id = CAST(ds_subjects_phenotypes_baseline._baseline_data->>'data_version' AS INT)
    WHERE ds_subjects_phenotypes_baseline.subject_type = 'PSP/CDB'
    ORDER BY subject_id;