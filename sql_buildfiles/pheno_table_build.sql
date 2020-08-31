/* Table definitions for phenotype database*/

/*universal enums*/
CREATE TYPE "public"."apoe" AS ENUM('22', '23', '24', '33', '34', '44', 'NA');            
CREATE TYPE "public"."autopsy" AS ENUM('0', '1', 'NA');            
CREATE TYPE "public"."braak" AS ENUM('0', '1', '2', '3', '4', '5', '6', 'NA');            
CREATE TYPE "public"."race" AS ENUM('0', '1', '2', '3', '4', '5', '6');            
CREATE TYPE "public"."ethnicity" AS ENUM('0', '1', 'NA');            

/*cc enum defs*/
CREATE TYPE "public"."prevAD" AS ENUM('0', '1', 'NA');            
CREATE TYPE "public"."incAD" AS ENUM('0', '1', 'NA');            
CREATE TYPE "public"."cc_ad" AS ENUM('0', '1', 'NA');            

/*fam enum defs*/
CREATE TYPE "public"."fam_ad" AS ENUM('0', '1', '2', '3', '4', '5', '9', '10');            
CREATE TYPE "public"."fam_famgrp" AS ENUM('1', '2', '3', '4', '5');            


CREATE TABLE IF NOT EXISTS "data_versions" 
    (
        "id" SERIAL NOT NULL, 
        "version_date" VARCHAR(25) NOT NULL, 
        "createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        "updatedat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, 
        PRIMARY KEY ("id")
    );

CREATE TABLE IF NOT EXISTS "subjects_phenotypes_cc" 
    (
        "id" SERIAL NOT NULL, /*pkey for table, not subject_id*/
        "subject_id" VARCHAR(50) NOT NULL,  
        "sex" BOOLEAN NOT NULL,
        "prevAD" "public"."prevAD" DEFAULT 'NA', 
        "incAD" "public"."incAD" DEFAULT 'NA',
        "age" VARCHAR(3) NOT NULL DEFAULT 'NA',
        "age_baseline" VARCHAR(3) NOT NULL DEFAULT 'NA',
        "apoe" "public"."apoe" DEFAULT 'NA',
        "autopsy" "public"."autopsy" NOT NULL DEFAULT 'NA',
        "braak" "public"."braak" NOT NULL DEFAULT 'NA',
        "race" "public"."race" NOT NULL,
        "ethnicity" "public"."ethnicity" NOT NULL DEFAULT 'NA',
        "selection" BOOLEAN NOT NULL,
        "AD" "public"."cc_ad" NOT NULL DEFAULT 'NA',
        "comments" VARCHAR(500),
        "data_version" INTEGER REFERENCES "data_versions" ("id") ON DELETE SET NULL ON UPDATE CASCADE, 
        "createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        "updatedat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, 
        PRIMARY KEY ("id")
    );
														

CREATE TABLE IF NOT EXISTS "subjects_phenotypes_family" 
    (
        "id" SERIAL NOT NULL, /*pkey for table, not subject_id*/
        "subject_id" VARCHAR(50) NOT NULL,
        "family_id" VARCHAR(50), 
        "mother_id" VARCHAR(50) DEFAULT '0',
        "father_id" VARCHAR(50) DEFAULT '0',
        "sex" BOOLEAN NOT NULL,
        "AD" "public"."fam_ad" NOT NULL,
        "age" VARCHAR(3) NOT NULL DEFAULT 'NA',
        "age_baseline" VARCHAR(3) NOT NULL DEFAULT 'NA',
        "apoe" "public"."apoe" DEFAULT 'NA',
        "autopsy" "public"."autopsy" NOT NULL DEFAULT 'NA',
        "braak" "public"."braak" NOT NULL DEFAULT 'NA',
        "race" "public"."race" NOT NULL,
        "ethnicity" "public"."ethnicity" NOT NULL DEFAULT 'NA',
        "family_group" "public"."fam_famgrp" NOT NULL,
        "comments" VARCHAR(500),
        "data_version" INTEGER REFERENCES "data_versions" ("id") ON DELETE SET NULL ON UPDATE CASCADE NOT NULL, 
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
        "createdat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        "updatedat" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, 
        PRIMARY KEY ("id")
    );
