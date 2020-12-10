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
data_version = ''
publish_status = False

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global user_input_subject_type
    global publish_status
    global data_version
    
    # user_input_subject_type = get_subject_type()
    
    # publish_status = get_publish_action()
    
    # LOADFILE = get_filename()

    # data_version = user_input_data_version()

    data_version = 'ng00067.v4'
    user_input_subject_type = 'case/control'
    publish_status =True
    LOADFILE = 'starting_data_for_test.csv'

    
    print('start ', datetime.datetime.now())
    data_dict = create_data_dict(LOADFILE)

    write_to_db(data_dict)
    

def create_data_dict(LOADFILE):
    """takes loadfile name as arg, returns dict of json data keyed by subject id of data to be entered in database"""
    global user_input_subject_type
    global publish_status
    global data_version

    release_dict = build_release_dict()
    dupecheck_list = build_dupecheck_list( release_dict[ data_version ], publish_status, user_input_subject_type )
    
    data_dict = {}

    with open(f'./source_files/{LOADFILE}', mode='r', encoding='utf-8-sig') as csv_file:
        """"get the relationship table names and indexes from the csv file headers"""
        pheno_file = csv.reader(csv_file)
        headers = next(pheno_file)
        
        for row in pheno_file:
            subject_id = row[ 0 ]
            if pheno_file.line_num > 1:
                blob = {}

                for index, value in enumerate( row ):

                    try:
                        blob[headers[ index ].lower()] = int( value )
                    except:
                        blob[headers[ index ].lower()] = value

                    blob[ "data_version" ] = release_dict[ data_version ]

                if type(blob["data_version"]) == int:
                    if check_not_duplicate( blob[ "subjid" ], dupecheck_list ):
                        data_dict[f'{ blob["subjid"] }_{ data_version }'] = blob
                    else:
                        print(f'{ blob["subjid"] } already has record in { data_version }')
                else:
                    print(f"Version { blob['data_version'] } not found. Record will not be added. Check database.")


    for key, record in data_dict.items():
        """remove subject id from blob for each record in dict"""
        record.pop('subjid')

    return data_dict

def write_to_db(data_dict):
    """takes data dict and writes to database"""

    requires_ad_status_check = ['case/control', 'family']
    requires_diagnosis_update_check = ['ADNI', 'PSP/CDB']

    global user_input_subject_type
    global publish_status
    ## gets list of subjects in baseline table of type matching the user_input_subject_type, so can see if have to add to baseline table
    baseline_dupecheck_list = build_baseline_dupcheck_list( user_input_subject_type )

    ## dicts for dbcall-less flag updates
    update_baseline_dict = build_update_baseline_check_dict( user_input_subject_type )
    update_latest_dict = build_update_latest_dict( user_input_subject_type )

    if user_input_subject_type in requires_ad_status_check:
        adstatus_check_dict = build_adstatus_check_dict( user_input_subject_type )

    for key, value in data_dict.items():
        """key is id + version, so cuts off version part to get id"""
        split = key.index("_")
        subject_id = key[:split]

        #have to add these to data here because otherwise will always show as "new not in database"
        value["update_baseline"] = update_baseline_check( subject_id , value, update_baseline_dict )
        value["update_latest"] = update_latest_check( subject_id, value, update_latest_dict )
        value["correction"] = correction_check( value )

        value["update_adstatus"] = 0

        if user_input_subject_type in requires_ad_status_check:
            value["update_adstatus"] = update_adstatus_check( subject_id, value[ "ad" ], adstatus_check_dict )     

        # if user_input_subject_type in requires_diagnosis_update_check:
        #     value["update_diagnosis"] = update_diagnosis_check( subject_id, user_input_subject_type, value )

        _data = json.dumps( value )
        # breakpoint(print('before db call'))
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