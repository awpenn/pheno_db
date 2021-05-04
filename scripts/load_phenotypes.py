import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json
import datetime

import calendar
import time

import flagchecks
import pheno_utils

new_records = []
success_id_log = []
error_log = {}

user_input_subject_type = ''
script_name = 'load_phenotypes.py'

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global user_input_subject_type

    print(
        "This script is intended for loading legacy (already published) phenotype data.\n \
If you are loading phenotype updates not currently in the database, please use the `load_unpublished_updates` script. \n \
If you are uploading changes to updates already in the database, but which are not yet ready to publish, please use the `manage_phenotypes` script. " 
    )
    
    time.sleep(5)

    user_input_subject_type = pheno_utils.get_subject_type( )

    data_version = pheno_utils.user_input_data_version( )

    
    LOADFILE = pheno_utils.get_filename( )

    start_time = datetime.datetime.now( )
    print( 'start ', start_time )

    variables_match_dictionary, msg = pheno_utils.check_loadfile_correctness( LOADFILE, user_input_subject_type )
    
    if not variables_match_dictionary:
        print( msg )
        sys.exit()
        
    else:
        print( msg )
        ## 12/15 create_data_dict generalized and moved to utils
        data_dict = pheno_utils.create_data_dict( LOADFILE, user_input_subject_type, data_version, script_name )
        write_to_db( data_dict, data_version )

    print('end ', datetime.datetime.now())
    print(f"( Program started { start_time } )")
    
def write_to_db( data_dict, data_version_string ):
    """takes data dict and writes to database"""

    requires_ad_status_check = [ 'case/control', 'family' ]
    requires_diagnosis_update_check = [ 'ADNI', 'PSP/CDB' ]

    global user_input_subject_type
    data_version = None ## saving data version id from dict so can set publish status in data_version table after upload
    write_counter = 0

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

    for key, value in data_dict.items():
        if 'subjid' in value.keys( ):
             subject_id = str( value.pop( 'subjid' ) )
        elif 'subject_id' in value.keys( ):
             subject_id = str( value.pop( 'subject_id' ) )

        #have to add these to data here because otherwise will always show as "new not in database"
        value[ "update_baseline" ] = flagchecks.update_baseline_check( subject_id , value, update_baseline_dict )
        value[ "update_latest" ] = flagchecks.update_latest_check( subject_id, value, update_latest_dict )
        value[ "correction" ] = flagchecks.correction_check( value )

        if user_input_subject_type in requires_ad_status_check:
            value[ "update_adstatus" ] = flagchecks.update_adstatus_check( subject_id, value[ "ad" ], adstatus_check_dict )     

        if user_input_subject_type in requires_diagnosis_update_check:
            value[ "update_diagnosis" ] = flagchecks.update_diagnosis_check( subject_id, user_input_subject_type, value, diagnosis_update_check_dict )

        _data = json.dumps( value )

        try:
            pheno_utils.database_connection( f"INSERT INTO ds_subjects_phenotypes(subject_id, _data, subject_type, published) VALUES(%s, %s, '{ user_input_subject_type }', TRUE)", ( subject_id, _data ) )
            write_counter += 1
            
            try:
                pheno_utils.save_baseline( baseline_dupecheck_list, subject_id, value, user_input_subject_type )
            except:
                print(f"Error making baseline entry for { subject_id } in { data_version_string }")

            

        except:
            print(f"Error making entry for { subject_id } in { data_version_string }")

    ## ask user if ready to publish dataset, if yes, will flip publish boolean in data versions table
    if write_counter > 0:
        if pheno_utils.user_input_publish_dataset( data_version_string, write_counter ):
            pheno_utils.change_data_version_published_status( "TRUE", data_version )
        else:
            print( f"Phenotype records published for { data_version_string } and cannot be changed, but version not published." )
            pheno_utils.change_data_version_published_status( "FALSE", data_version )
    else:
        print( "No records entered in database." )

if __name__ == '__main__':
    main()
    # pheno_utils.generate_errorlog()
    # pheno_utils.generate_success_list()