import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json
import datetime

import calendar
import time

from flagchecks import *
from pheno_utils import *
from initial_validation_checks import *

new_records = []
success_id_log = []
error_log = {}

user_input_subject_type = ''
publish_status = False
script_name = 'load_phenotypes.py'

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global user_input_subject_type
    global publish_status

    user_input_subject_type = get_subject_type()
    
    data_version = user_input_data_version()

    publish_status = get_publish_action()
    
    LOADFILE = get_filename()

    print('start ', datetime.datetime.now())

    variables_match_dictionary, msg = check_loadfile_correctness( LOADFILE, user_input_subject_type )
    
    if not variables_match_dictionary:
        print( msg )
        sys.exit()
        
    else:
        print( msg )
        ## 12/15 create_data_dict generalized and moved to utils
        data_dict = create_data_dict( LOADFILE, user_input_subject_type, publish_status, data_version, script_name )
        write_to_db( data_dict )


def write_to_db(data_dict):
    """takes data dict and writes to database"""

    requires_ad_status_check = [ 'case/control', 'family' ]
    requires_diagnosis_update_check = [ 'ADNI', 'PSP/CDB' ]

    global user_input_subject_type
    global publish_status
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
        subject_id = value.pop('subjid')

        #have to add these to data here because otherwise will always show as "new not in database"
        value[ "update_baseline" ] = update_baseline_check( subject_id , value, update_baseline_dict )
        value[ "update_latest" ] = update_latest_check( subject_id, value, update_latest_dict )
        value[ "correction" ] = correction_check( value )

        if user_input_subject_type in requires_ad_status_check:
            value[ "update_adstatus" ] = update_adstatus_check( subject_id, value[ "ad" ], adstatus_check_dict )     

        if user_input_subject_type in requires_diagnosis_update_check:
            value[ "update_diagnosis" ] = update_diagnosis_check( subject_id, user_input_subject_type, value, diagnosis_update_check_dict )

        _data = json.dumps( value )

        database_connection(f"INSERT INTO ds_subjects_phenotypes(subject_id, _data, subject_type, published) VALUES('{ subject_id }', '{ _data } ', '{ user_input_subject_type }', { publish_status })")
        save_baseline( baseline_dupecheck_list, subject_id, value )

def create_baseline_json( data ):
    """takes dict entry for subject being added to database and creates the copy of data for baseline table, returning json string"""
    baseline_data = {}
    for key, value in data.items():
        if "update" not in key and "correction" not in key:
            baseline_data[key] = value
 
    return json.dumps( baseline_data )

def save_baseline( baseline_dupecheck_list, subject_id, data ):
    """takes data dict and writes to database"""
    global user_input_subject_type

    _baseline_data = create_baseline_json( data )

    if check_not_dupe_baseline( subject_id , baseline_dupecheck_list ):
        database_connection(f"INSERT INTO ds_subjects_phenotypes_baseline(subject_id, _baseline_data, subject_type) VALUES('{subject_id}', '{_baseline_data}', '{user_input_subject_type}')") 
    else:
        print(f'There is already a {user_input_subject_type} baseline record for {subject_id}.')



if __name__ == '__main__':
    main()
    print('end ', datetime.datetime.now())
    # generate_errorlog()
    # generate_success_list()