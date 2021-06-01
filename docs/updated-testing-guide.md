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

## new workflow

1. In main menu, run `create new release`.  You'll choose the data type, and assign a release name and date, and the script will create copies of the subjects from the latest published version, now assigned to the new release.  

2. Drop subjects, load new subjects, load changes to existing subjects' phenotypes
    - a. Delete subjects dropped from new release
        - Use `drop_subject_tempate` ( in jupyterhub `templates` folder ) file to create a batch file for subjects to be dropped. You'll provide subject_ids and the release_name of the new release you created in step 1. 

    - b. Load new subjects/updates to existing subjects
        - create a file ( using the template appropriate for your data type ) containing only phenotype records for subjects not in the previous release AND subjects from the previous release who have updated phenotypes in the new release.
        - subjects not in previous releases will have new records created for them, subjects copied over from the previous release, but which have updated phenotypes, will have their existing database record updated.

3. Generate the comparison file
    - the generated comparison file will have an `is_change` column containing a TRUE/FALSE value, indicating whether there are any changes for that subjects between the latest published/baseline record and the latest update.  You can use this to filter records to see only those that have changed phenotype values. 

4. (optional for testing I think) Run update validation checks  

5. Load corrections/changes to update:
    - create copy of comparison file
    - remove the following columns from the copy:
        - all beginning with `prev_` ( ie. those showing the current version of a phenotype valuePUBLISHED in the database )
        - `data_version` columns ( you'll select this manually in the script )
        - all comparison columns ( ie. those at end of table showing changes between update andpublished )
        - `data_errors`  
    - add the `duplicate_subjid` column back in 
    - select `Makes changes to updates` from main menu  
    
6. Publish the new release
    - if you are loading changes to update records ( step 5 ), you can choose to publish the release at the end of the script running
    - if you are ready to publish the release and have no corrections/changes to load, select `publish new release` from the main menu