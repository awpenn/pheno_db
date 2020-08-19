/*DROP AND RECREATE*/
DROP VIEW IF EXISTS lookup_aliases;
DROP VIEW IF EXISTS builder_lookup;
DROP VIEW IF EXISTS lookup_cc;
DROP VIEW IF EXISTS lookup_fam;
DROP VIEW IF EXISTS subjects_samples;
DROP VIEW IF EXISTS subjects_samples_ids;
DROP VIEW IF EXISTS lookup;

/*Views creation*/

	/*create lookup view to be able to filter ids by identifier code for cohort*/

		CREATE VIEW lookup AS
		SELECT generated_ids.id AS id, adsp_id, site_fam_id, site_indiv_id, cohort_identifier_codes.cohort_identifier_code, lookup_id, adsp_family_id, adsp_indiv_partial_id, comments, subject_type, generated_ids.createdat 
		FROM generated_ids
		JOIN cohort_identifier_codes
		ON generated_ids.cohort_identifier_code_key = cohort_identifier_codes.id WHERE "valid" = TRUE;

	/*create lookup view with no valid filter for checker script*/

		CREATE VIEW builder_lookup AS
		SELECT generated_ids.id AS id, adsp_id, site_fam_id, site_indiv_id, cohort_identifier_codes.cohort_identifier_code, lookup_id, adsp_family_id, adsp_indiv_partial_id, comments, subject_type
		FROM generated_ids
		JOIN cohort_identifier_codes
		ON generated_ids.cohort_identifier_code_key = cohort_identifier_codes.id;
	
	/*create view to filter by case/control subjects*/

		CREATE VIEW lookup_cc AS
		SELECT generated_ids.id AS id, adsp_id, site_fam_id, site_indiv_id, cohort_identifier_codes.cohort_identifier_code, lookup_id, adsp_family_id, adsp_indiv_partial_id, comments, subject_type
			FROM generated_ids
			JOIN cohort_identifier_codes
			ON generated_ids.cohort_identifier_code_key = cohort_identifier_codes.id WHERE "subject_type" = 'case/control' AND "valid" = TRUE;
		
	/*create view to filter by family subjects*/

		CREATE VIEW lookup_fam AS
		SELECT generated_ids.id AS id, adsp_id, site_fam_id, site_indiv_id, cohort_identifier_codes.cohort_identifier_code, lookup_id, adsp_family_id, adsp_indiv_partial_id, comments, subject_type
			FROM generated_ids
			JOIN cohort_identifier_codes
			ON generated_ids.cohort_identifier_code_key = cohort_identifier_codes.id WHERE "subject_type" = 'family' AND "valid" = TRUE;
		
	/*create view to generated table of records in generated_ids that have associated alias ids*/
		
		CREATE VIEW lookup_aliases AS
		SELECT alias_site_indiv_id as alias_id, generated_ids.site_indiv_id, generated_ids.lookup_id, generated_ids.adsp_id, cohort_identifier_codes.cohort_identifier_code
			FROM alias_ids
			JOIN generated_ids
				ON alias_ids.generated_ids_lookup_id=generated_ids.lookup_id
			JOIN cohort_identifier_codes
				ON generated_ids.cohort_identifier_code_key=cohort_identifier_codes.id
			WHERE alias_ids.generated_ids_lookup_id=generated_ids.lookup_id
				AND alias_ids.cohort_identifier_code_key=generated_ids.cohort_identifier_code_key;	
	
	/*create view to generate table of data from generated_ids table along with corresponding sample_ids*/

		CREATE VIEW subjects_samples AS
			SELECT adsp_id, sample_id, lookup_id, data_type, cohort_identifier_code, sequencing_study, subject_type, comments
			FROM lookup
			JOIN sample_ids
			ON lookup.adsp_id = sample_ids.subject_adsp_id;
		
	/*create view to generate table of subject_ids, cohort codes, and corresponding sample_ids*/

		CREATE VIEW subjects_samples_ids AS
			SELECT adsp_id, sample_id, cohort_identifier_code
			FROM lookup
			JOIN sample_ids
			ON lookup.adsp_id = sample_ids.subject_adsp_id;