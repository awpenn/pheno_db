# The pheno_db worfklow
## Preparing Data
- Templates for phenotype uploads are in the `READMEs and templates` folder
    - Use the template corresponding to the type of data being uploaded ( ie. case/control, ADNI, family, etc.)
    - The `duplicate_subjid` is not found in existing data, but will be(?) going forward, and is included in the current data dictionaries. As such, it must be in the upload sheets, but won't appear in the generated data views. *for now*, just include a `0` value for each subject in the upload sheet
    - do not remove the column headers
    - make sure the loadfile doesn't have any trailing empty rows, and save as a .csv ( not CSV UTF-8 )
    - upload your completed upload sheet to the `source_files` folder in the hub

## Tables and Views in the database
### Tables
- `ds_subjects_phenotypes` and `ds_subjects_phenotypes_baseline`
    - here is where raw json representing the phenotype data is stored in the database.  Each row has a subject_id, that subject's corresponding phenotype data collapsed into a single `jsonb` column, and a `published` status for that particular record.  The json data can be queried using postgresql's `->>` operator, but it's probably easier and more visually comprehensible to use the various views created to break out json data into table form.  
    - `ds_subjects_phenotypes` will potentially have several records for a given subject, while `ds_subjects_phenotypes_baseline` will only have one phenotype record per subject in a given data_type.
- `data_versions`
    - Contains basic release_version data ( its release date, the name accession number-like name of the release, and its publication status ).  When adding data for a new release, the user will have to first enter the version information in this table ( by default the publication status will be set to FALSE )
- `env_var_by_subject_type`
    - Contains variables for the different data_types, used in the data processing scripts. Values in this table should not be changed by anyone besides the administrator.  

### Views
- `_get_current_data`, `_get_newest_data`, `_get_unpublished_updates_data`
    - these views sort data from the `ds_subjects_phenotypes` table, to be further sorted by datatype and broken out into table from in the subsequent child views. Data is still in json, so again, not terribly useful.

- `[data_type]_all`: Returns all records for a given data_type, grouped by subject_id and ordered by version
- `[data_type]_current`: Returns most recent published phenotype record for given data_type.
- `[data_type]_newest`: Returns newest phenotype record for given data_type.
- `[data_type]_unpublished_updates`: Returns phenotype records with FALSE `published` value, from data_versions which are also of FALSE `published` status, for given data_type.  

## Data States in the phenotype database
- Data in the database moves though three stages or "states" as we move through the workflow.
    - `record UNPUBLISHED, data version UNPUBLISHED`: when data is first uploaded into the database it is "unpublished", meaning that the individual record has a `published` value of FALSE in the `ds_subjects_phenotypes` table, and the data_version to which the record belongs is also has a `published` value of FALSE in the `data_versions` table.  These records will appear in the `[data_type]_unpublished_updates` view, but not in the `data_type]_current` view.
    
    - `record PUBLISHED, data version UNPUBLISHED`: After validation and review, a subject's new phenotype record will be "published" in the sense that its `published` value will be set to TRUE is the `ds_subjects_phenotypes` table and no further revisions to the update will be allowed for that particular version. However, it may be that case that you're working in batch, and therefore not ready to "publish" the entire data_version ( analogous to "releasing" a data version ).  Therefore, after each batch of finalized updates is published, the user is asked if they then wish to publish the entire data version.  If the user chooses not to at that time, the data version in the `data_versions` table will remain with `published` value = FALSE, and the new phenotype record will not appear in the `[data_type]_current` or `[data_type]_unpublished_updates` views, but will appear in the `[data_type]_newest` view.

    - `record PUBLISHED, data version PUBLISHED`:  Once the last batch of finalized updates are uploaded and published, the user will additionaly choose to published the entire data version.  The `published` value in the `data_versions` table will be set to TRUE, and the subject's "most current" phenotypes will reflect the newest updated record.  The record will appear in the `[data_type]_current` and `[data_type]_current` views, and will no longer appear in the `[data_type]_unpublished_updates` view. 

## Phenotype management actions
- In jupyterhub, open the `manage_phenotypes` notebook
- Running the notebook script, you will be presented with a number of actions to take. 
    - broadly, you will use `Load unpublished phenotypes` to load completely new update records into the database ( eg. a subject has published phenotypes for v2, but no existing records, published or otherwise, currently in the database. ) and `Manage updates` to upload revisions to existing unpublished records and ultimately to publish the finalized version of those records. 
    - once a given subject has at least one published version and an unpublished update version of their phenotypes, you will use `Generate Comparison file` to create a comparison with the update and latest published version of phenotypes or with the baseline version of a subject's phenotypes.  

## Update Workflow
*( This is the current envisioned workflow conceived during database development. Part of testing will be confirming that this workflow makes sense, so as you move through this workflow, let me know if you have any concerns or suggestions for revision)*  

- Upload update csv to `source_files` folder in jupyterhub  

- Run `Run initial validation checks`
    - resolve conflicts  

- Run `Load unpublished phenotypes`
    - Select data_type ( case/control, family, ADNI, PSP/CDB )
    - select data_version ( options restricted to unpublished data versions )
    - check `[data_type]_unpublished_updates` to confirm upload  

- Run `Generate comparison file`
    - Select type of comparison to be run
    - generated file will appear in the `comparison_files` folder
    - inspect the comparison file  

- Copy generated file into the `source_files` folder  

- Run `Validate phenotype update data`
    - If no errors are found or flags raised, no file will be created, and a message indicating such will be displayed
    - if errors are found or flags raised, a data errors file will be generated
    - inspect the error file
    - nb. in my mind, this error file will be used to track resolution of errors/issues
        - this will then be used to create an entirely new upload file once all the issues are resolved, or changes can be made to this tracking file as you go.  

- Once the validation script returns no errors, or all flags raised are determined to be of no concern:
    - either generate an entirely new upload file based on the template, or if changes have been made to the comparison file:
        - make a copy of the file
        - remove the following columns from the copy:
            - all beginning with `prev_` ( ie. those showing the current version of a phenotype value PUBLISHED in the database )
            - `data_version` columns ( you'll select this manually in the script )
            - all comparison columns ( ie. those at end of table showing changes between update and published )
            - `data_errors`  
            
- Upload new file to the `source_files` folder  

- Run `Run intial validation check` again?  

- Run `Manage updates`
    - Enter data_type, data_version 
    - You'll be asked if you want to publish the loaded data
        - this depends on how we choose to handle the data, to my thinking, if we have a number of subjects with errors/flags in their updates, do we upload corrections to unpublished updates as they get resolved (data will not be published), or do we keep them all in a tracking file and then upload them when we have their final versions (data will be published). 
    - The script will run its checks for duplicates, generate tracking flags, and update the appropriate existing update record in the `ds_subjects_phenotypes` table with it's finalized phenotype data, switching the record's `published` status to TRUE.
        - once an update record has been published, no further revisions to that particular record are allowed
    - If publishing records
        - once all the records in the upload file are published in the database, the user will be asked if they want to additionally publish the entire data version. If yes, the `published` value in the `data_versions` table will be set to TRUE, and no further data can be uploaded or changed for that data version.   

 - Check the [data_version]_current view to confirm data completeness and correctness.  

