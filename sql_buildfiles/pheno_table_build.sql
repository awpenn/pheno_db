/* Create enum list for subject_type*/
CREATE TYPE "public"."subject_type" AS ENUM('case/control', 'family', 'other');       

/*Create tables for adspid_id database*/

	/*cohort identifier code table*/
	CREATE TABLE IF NOT EXISTS "cohort_identifier_codes" (
		"id" SERIAL NOT NULL,
		"cohort_identifier_code" VARCHAR(10) NOT NULL,
		"full_sitename" VARCHAR(100),
		"description" VARCHAR (100),
		"adsp_id_leading_letter" VARCHAR(1) NOT NULL,
		"adsp_generated_ids_prefix" VARCHAR(10) NOT NULL,
		"createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
		PRIMARY KEY ("id")
	);
	
	/*main adspid table*/
	CREATE TABLE IF NOT EXISTS "generated_ids" (
		"id" SERIAL NOT NULL,
			--PK for table
		"site_fam_id" VARCHAR(50),
			--non-ADSP family id (site specific)
		"site_indiv_id" VARCHAR (50),
			--non-ADSP individual id (site specific)
		"cohort_identifier_code_key" INTEGER REFERENCES "cohort_identifier_codes" ("id")
			ON DELETE SET NULL ON UPDATE CASCADE,
			--FKEY for lettered code assigned to cohort from `cohort_identifier_code` table
		"lookup_id" VARCHAR(50),
			--non-ADSP combined family and individual ids
		"adsp_family_id" VARCHAR(50),
		"adsp_indiv_partial_id" VARCHAR(50),
			--unique part of generated ADSP ID for individual
		"adsp_id" VARCHAR(50) UNIQUE,
		"comments" VARCHAR (500),
		"valid" BOOLEAN NOT NULL DEFAULT TRUE, 
			--boolean indicating whether id is valid.
		"subject_type" "public"."subject_type" DEFAULT 'other',
			--enum list with values case/control, family, and other, indicating subject is member of case/control or family study
		"createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
		PRIMARY KEY("id")
	);
	
	/*create table to manage tracking alias ids for records in generated_ids, ie. when data comes in from site where there are multiple site ids for the same
	subject, one is chosen and given a record in generated_ids, but the other ids still need to be recorded*/

	CREATE TABLE IF NOT EXISTS "alias_ids"(
		"id" SERIAL NOT NULL,
			--PK for table
		"alias_site_indiv_id" VARCHAR(50) NOT NULL,
			--id that doesn't serve as primary site_indiv_id for a record in generated_ids table, but which is still associated with said record as alias id.
		"generated_ids_lookup_id" VARCHAR(50),
			--lookup_id for record in generated_ids to which the alias_site_indiv_id is linked.
		"cohort_identifier_code_key" INTEGER REFERENCES "cohort_identifier_codes" ("id")
		    --FK from cohort_identifier_codes table for cohort
	);
	/*Create table to manage sample_ids, recording the externally-generated id, datatype of the sample, and name of study the sample was sequenced in. */
	CREATE TABLE IF NOT EXISTS "sample_ids"(
		"id" SERIAL NOT NULL,
		--PK FOR TABLE
		"sample_id" VARCHAR(50) UNIQUE,
		--id for a particular sample, generated from concatenation of repository id, tissue source, aliqout-id and subject's adsp-generated id.
		"data_type" VARCHAR(25),
		--molecular datatype (e.g. WGS, WES, etc.) of sample.
		"sequencing_study" VARCHAR(50),
		--study for which the sample was sequenced.
		"subject_adsp_id" VARCHAR (50) REFERENCES "generated_ids" ("adsp_id"),
		--adsp id for the subject from whom the sample was derived.
		"createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
	);