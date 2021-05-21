import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

import flagchecks
import pheno_utils

import calendar
import time

user_input_subject_type = 'other'
publish_status = False
unpublished_subjects_in_database_dict = None
script_name = 'manage_updates.py'

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global user_input_subject_type
    global publish_status
    global unpublished_subjects_in_database_dict

    ## checks if DEBUG arg passed in script call, sets DEBUG variable to True if so
    pheno_utils.check_DEBUG( )

    user_input_subject_type = pheno_utils.get_subject_type()

    unpublished_subjects_in_database_dict = pheno_utils.get_unpublished_subjects_by_release( subject_type = user_input_subject_type )

    data_version = pheno_utils.user_input_data_version()

    publish_status = pheno_utils.get_publish_action()

    LOADFILE = pheno_utils.get_filename()
    
    variables_match_dictionary, msg = pheno_utils.check_loadfile_correctness( LOADFILE, user_input_subject_type )
    
    if not variables_match_dictionary:
        print( msg )
        sys.exit()
        
    else:
        print( msg )
        data_dict = pheno_utils.create_data_dict( LOADFILE, user_input_subject_type, data_version, script_name )
        write_to_db( data_dict, data_version )

    pheno_utils.generate_errorlog( )

def write_to_db( data_dict, data_version_string ):
    """takes data dict and string version of data version and writes to database"""
    requires_ad_status_check = ['case/control', 'family']
    requires_diagnosis_update_check = ['ADNI', 'PSP/CDB']

    global user_input_subject_type
    global publish_status
    global unpublished_subjects_in_database_dict

    data_version = None ## saving data version id from dict so can set publish status in data_version table after upload
    write_counter = 0
    
    ## check if subject has unpublished record for this data version in database, log error and remove from data_dict if DOESN'T
    for key, subject in list( data_dict.items( ) ):
        if 'subjid' in subject.keys( ):
            sid = subject[ 'subjid' ]
        elif 'subject_id' in subject.keys( ):
            sid = subject[ 'subject_id' ]
        
        if sid not in unpublished_subjects_in_database_dict[ data_version_string ][ 'unpublished_subjects' ]:
            del data_dict[ key ]
            err = f'ERROR: { sid } has no unpublished record in { data_version_string }. No action will be taken.'
            print( err )
            pheno_utils.error_log[ len( pheno_utils.error_log ) + 1 ] = [ err ]

    ## gets list of subjects in baseline table of type matching the user_input_subject_type, so can see if have to add to baseline table
    baseline_dupecheck_list = pheno_utils.build_baseline_dupcheck_list( user_input_subject_type )

    ## dicts for dbcall-less flag updates
    update_baseline_dict = flagchecks.build_update_baseline_check_dict( user_input_subject_type )
    update_latest_dict = flagchecks.build_update_latest_dict( user_input_subject_type )

    if user_input_subject_type in requires_ad_status_check:
        adstatus_check_dict = flagchecks.build_adstatus_check_dict( user_input_subject_type )
    
    if user_input_subject_type in requires_diagnosis_update_check:
        diagnosis_update_check_dict = flagchecks.build_update_diagnosis_check_dict( user_input_subject_type )

    for key, value in data_dict.items():
        data_version = value[ "data_version" ] ## saving data version id from dict so can set publish status in data_version table after upload

        if 'subjid' in value.keys(): ## handling for subjid/subject_id inconsistency
            subject_id = str( value.pop( "subjid" ) )
        elif 'subject_id' in value.keys():
            subject_id = str( value.pop( "subject_id" ) )
            
        version = value["data_version"]

        #have to add these to data here because otherwise will always show as "new not in database"
        value[ "update_baseline" ] = flagchecks.update_baseline_check( subject_id , value, update_baseline_dict )
        value[ "update_latest" ] = flagchecks.update_latest_check( subject_id, value, update_latest_dict )
        value[ "correction" ] = flagchecks.correction_check( value )

        if user_input_subject_type in requires_ad_status_check:
            value[ "update_adstatus" ] = flagchecks.update_adstatus_check( subject_id, value[ "ad" ], adstatus_check_dict )     

        if user_input_subject_type in requires_diagnosis_update_check:
            value[ "update_diagnosis" ] = flagchecks.update_diagnosis_check( subject_id, user_input_subject_type, value, diagnosis_update_check_dict )

        _data = json.dumps( value )

        if publish_status:
            ## update an existing unpublished record (WHERE published = FALSE), setting it to true
            if not pheno_utils.DEBUG:
                try:
                    pheno_utils.database_connection(f"UPDATE ds_subjects_phenotypes SET(subject_id, _data, published) = (%s, %s, TRUE) WHERE subject_id = %s AND subject_type = '{ user_input_subject_type }' AND _data->>'data_version' = '%s' AND published = FALSE", ( subject_id, _data, subject_id, version ) )
                    write_counter += 1
                except:
                    err = f'ERROR: Error updating phenotype record for { subject_id } in { data_version_string }'
                    print( err )
                    pheno_utils.error_log[ len( pheno_utils.error_log ) + 1 ] = [ err ]

            if not pheno_utils.DEBUG:          
                try:
                    pheno_utils.save_baseline( baseline_dupecheck_list, subject_id, value, user_input_subject_type )
                except:
                    err = f"ERROR: Error making baseline entry for { subject_id } in { data_version_string }"
                    print( err )
                    pheno_utils.error_log[ len( pheno_utils.error_log ) + 1 ] = [ err ]

        else:
            try:
                if not pheno_utils.DEBUG:
                    pheno_utils.database_connection( f"UPDATE ds_subjects_phenotypes SET(subject_id, _data) = (%s, %s) WHERE subject_id = %s AND subject_type = '{ user_input_subject_type }' AND _data->>'data_version' = '%s' AND published = FALSE", ( subject_id, _data, subject_id, version ) )
            except:
                err = f'ERROR: Error loading changes to update for { subject_id } in { data_version_string }'
                print( err )
                pheno_utils.error_log[ len( pheno_utils.error_log ) + 1 ] = [ err ]

        ## add subject_id back in for reporting
        data_dict[ key ][ 'subject_id' ] = subject_id

    if not pheno_utils.DEBUG:
        if write_counter > 0:
            if pheno_utils.user_input_publish_dataset( data_version_string, write_counter ):
                pheno_utils.change_data_version_published_status( "TRUE", data_version )
            else:
                print( f"Phenotype records published for { data_version_string } and cannot be changed, but version not published." )
                pheno_utils.change_data_version_published_status( "FALSE", data_version )
        else:
            print( "No added records were published." )
    
    ## report generation
    if publish_status:
        if data_dict:
            pheno_utils.generate_summary_report( data_dict = data_dict, user_input_subject_type = user_input_subject_type, loadtype = 'new_published_release' )
    else:
        pheno_utils.generate_summary_report( data_dict = data_dict, user_input_subject_type = user_input_subject_type, loadtype = 'unpublished_update' )
        
if __name__ == '__main__':
    main( )