import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

from flagchecks import *
from pheno_utils import *

import calendar
import time

new_records = []
success_id_log = []
error_log = {}

user_input_subject_type = 'other'
publish_status = False
script_name = 'manage_updates.py'

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global user_input_subject_type
    global publish_status

    user_input_subject_type = get_subject_type()

    data_version = user_input_data_version()

    publish_status = get_publish_action()

    LOADFILE = get_filename()
    
    variables_match_dictionary, msg = check_loadfile_correctness( LOADFILE, user_input_subject_type )
    
    if not variables_match_dictionary:
        print( msg )
        sys.exit()
        
    else:
        print( msg )
        data_dict = create_data_dict( LOADFILE, user_input_subject_type, data_version, script_name )
        write_to_db( data_dict, data_version )

def write_to_db( data_dict, data_version_string ):
    """takes data dict and string version of data version and writes to database"""
    requires_ad_status_check = ['case/control', 'family']
    requires_diagnosis_update_check = ['ADNI', 'PSP/CDB']

    global user_input_subject_type
    global publish_status

    data_version = None ## saving data version id from dict so can set publish status in data_version table after upload
    write_counter = 0
    
    ## gets list of subjects in baseline table of type matching the user_input_subject_type, so can see if have to add to baseline table
    baseline_dupecheck_list = build_baseline_dupcheck_list( user_input_subject_type )

    ## dicts for dbcall-less flag updates
    update_baseline_dict = build_update_baseline_check_dict( user_input_subject_type )
    update_latest_dict = build_update_latest_dict( user_input_subject_type )

    if user_input_subject_type in requires_ad_status_check:
        adstatus_check_dict = build_adstatus_check_dict( user_input_subject_type )
    
    if user_input_subject_type in requires_diagnosis_update_check:
        diagnosis_update_check_dict = build_update_diagnosis_check_dict( user_input_subject_type )

    for key, value in data_dict.items():
        data_version = value[ "data_version" ] ## saving data version id from dict so can set publish status in data_version table after upload

        if 'subjid' in value.keys(): ## handling for subjid/subject_id inconsistency
            subject_id = value.pop( "subjid" )
        else:
            subject_id = value.pop( "subject_id" )
        version = value["data_version"]

        #have to add these to data here because otherwise will always show as "new not in database"
        value[ "update_baseline" ] = update_baseline_check( subject_id , value, update_baseline_dict )
        value[ "update_latest" ] = update_latest_check( subject_id, value, update_latest_dict )
        value[ "correction" ] = correction_check( value )

        if user_input_subject_type in requires_ad_status_check:
            value[ "update_adstatus" ] = update_adstatus_check( subject_id, value[ "ad" ], adstatus_check_dict )     

        if user_input_subject_type in requires_diagnosis_update_check:
            value[ "update_diagnosis" ] = update_diagnosis_check( subject_id, user_input_subject_type, value, diagnosis_update_check_dict )

        _data = json.dumps( value )

        if publish_status:
            database_connection(f"UPDATE ds_subjects_phenotypes SET(subject_id, _data, published) = ('{ subject_id }', '{ _data }', TRUE) WHERE subject_id = '{ subject_id }' AND subject_type = '{ user_input_subject_type }' AND _data->>'data_version' = '{ version }' AND published = FALSE")
            write_counter += 1
        else:
            database_connection(f"UPDATE ds_subjects_phenotypes SET(subject_id, _data) = ('{ subject_id }', '{ _data }') WHERE subject_id = '{ subject_id }' AND subject_type = '{ user_input_subject_type }' AND _data->>'data_version' = '{ version }' AND published = FALSE")

    if write_counter > 0:
        if user_input_publish_dataset( data_version_string, write_counter ):
            change_data_version_published_status( "TRUE", data_version )
        else:
            print( f"Phenotype records published for { data_version_string } and cannot be changed, but version not published." )
            change_data_version_published_status( "FALSE", data_version )
    else:
        print( "No added records were published." )
        
if __name__ == '__main__':
    main()
    # generate_errorlog()
    # generate_success_list()