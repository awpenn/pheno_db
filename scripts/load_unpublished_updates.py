import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

import calendar
import time

import pheno_utils
import flagchecks

new_records = []

user_input_subject_type = ''
script_name = 'load_unpublished_updates.py'

def main():
    """main conductor function for the script.  Gets some user input about the type of data being uploaded and runs the process from there."""
    global user_input_subject_type

    ## checks if DEBUG arg passed in script call, sets DEBUG variable to True if so
    pheno_utils.check_DEBUG( )

    user_input_subject_type = pheno_utils.get_subject_type()

    data_version = pheno_utils.user_input_data_version()

    LOADFILE = pheno_utils.get_filename()

    variables_match_dictionary, msg = pheno_utils.check_loadfile_correctness( LOADFILE, user_input_subject_type )
    
    if not variables_match_dictionary:
        print( msg )
        sys.exit()
        
    else:
        print( msg )
        data_dict = pheno_utils.create_data_dict( LOADFILE, user_input_subject_type, data_version, script_name )

        if data_dict: ## ie. if any records were found that can be added to the database
            write_to_db( data_dict )
        else:
            print( "No records will be added to the database." )

def write_to_db( data_dict ):
    """takes data dict and writes to database"""

    requires_ad_status_check = ['case/control', 'family']
    requires_diagnosis_update_check = ['ADNI', 'PSP/CDB']
    
    global user_input_subject_type    

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
        if 'subjid' in value.keys( ):
             subject_id = str( value.pop( 'subjid' ) )
        elif 'subject_id' in value.keys( ):
             subject_id = str( value.pop( 'subject_id' ) )

        value[ "update_baseline" ] = flagchecks.update_baseline_check( subject_id , value, update_baseline_dict )
        value[ "update_latest" ] = flagchecks.update_latest_check( subject_id, value, update_latest_dict )
        value[ "correction" ] = flagchecks.correction_check( value )

        if user_input_subject_type in requires_ad_status_check:
            value[ "update_adstatus" ] = flagchecks.update_adstatus_check( subject_id, value[ "ad" ], adstatus_check_dict )     

        if user_input_subject_type in requires_diagnosis_update_check:
            value[ "update_diagnosis" ] = flagchecks.update_diagnosis_check( subject_id, user_input_subject_type, value, diagnosis_update_check_dict )

        _data = json.dumps( value )

        if not pheno_utils.DEBUG:
            try:
                pheno_utils.database_connection( f"INSERT INTO ds_subjects_phenotypes(subject_id, _data, subject_type) VALUES(%s, %s, '{ user_input_subject_type }')", ( subject_id, _data ) )
            except:
                err = f'Error adding update for { subject_id } to database.'
                print( err )
                pheno_utils.error_log[ len( pheno_utils.error_log ) + 1 ] = [ err ]
        else:
            print( 'DEBUG mode, no write to db....\n')
        ## add subject_id back in for reporting
        data_dict[ key ][ 'subject_id' ] = subject_id

    pheno_utils.generate_summary_report( data_dict = data_dict, user_input_subject_type = user_input_subject_type, loadtype = 'unpublished_update' )

if __name__ == '__main__':
    main()
    pheno_utils.generate_errorlog( )