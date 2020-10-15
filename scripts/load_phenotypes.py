import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

import calendar
import time

from flagchecks import *
from pheno_utils import *

new_records = []
success_id_log = []
error_log = {}

user_input_subject_type = ''
publish_status = False

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global user_input_subject_type
    global publish_status
    
    user_input_subject_type = get_subject_type()
    publish_status = get_publish_action()
    LOADFILE = get_filename()
    data_dict = create_data_dict(LOADFILE)
    write_to_db(data_dict)

def create_data_dict(LOADFILE):
    """takes loadfile name as arg, returns dict of json data keyed by subject id of data to be entered in database"""
    global user_input_subject_type
    data_dict = {}

    with open(f'./source_files/{LOADFILE}', mode='r', encoding='utf-8-sig') as csv_file:
        """"get the relationship table names and indexes from the csv file headers"""
        pheno_file = csv.reader(csv_file)
        headers = next(pheno_file)
        
        for row in pheno_file:
            if pheno_file.line_num > 1:
                blob = {}
                for index, value in enumerate(row):
                    try:
                        blob[headers[index].lower()] = int(value)
                    except:
                        blob[headers[index].lower()] = value
                    if headers[index].lower() == 'release_version':
                        blob["data_version"] = get_data_version_id(value)
                    if headers[index].lower() == 'latest_update_version':
                        blob["latest_update_version"] = get_data_version_id(value)
                    

                if type(blob["data_version"]) == int:
                    data_dict[f'{blob["subject_id"]}_{blob["release_version"]}'] = blob
                else:
                    print(f"Version {blob['data_version']} not found. Record will not be added. Check database.")


    for key, record in data_dict.items():
        """remove subject id from blob for each record in dict"""
        record.pop('subject_id')
        record.pop('release_version')

    return data_dict

def write_to_db(data_dict):
    """takes data dict and writes to database"""
    global user_input_subject_type
    global publish_status

    for key, value in data_dict.items():
        """key is id + version, so cuts off version part to get id"""
        split = key.index("_")
        subject_id = key[:split]

        #have to add these to data here because otherwise will always show as "new not in database"
        value["update_baseline"] = update_baseline_check( subject_id , user_input_subject_type , value )
        value["update_latest"] = update_latest_check( subject_id, user_input_subject_type, value )
        value["update_adstatus"] = update_adstatus_check( subject_id, user_input_subject_type, value )
        value["correction"] = correction_check( value )

        _data = json.dumps(value)

        database_connection(f"INSERT INTO ds_subjects_phenotypes(subject_id, _data, subject_type, published) VALUES('{subject_id}', '{_data}', '{user_input_subject_type}', {publish_status})")
        save_baseline(subject_id, value)

def create_baseline_json(data):
    """takes dict entry for subject being added to database and creates the copy of data for baseline table, returning json string"""
    baseline_data = {}
    for key, value in data.items():
        if "update" not in key and "correction" not in key:
            baseline_data[key] = value
 
    return json.dumps(baseline_data)

def save_baseline(subject_id, data):
    """takes data dict and writes to database"""
    global user_input_subject_type

    _baseline_data = create_baseline_json(data)

    if check_not_dupe_baseline( subject_id , user_input_subject_type ):
        database_connection(f"INSERT INTO ds_subjects_phenotypes_baseline(subject_id, _baseline_data, subject_type) VALUES('{subject_id}', '{_baseline_data}', '{user_input_subject_type}')") 
    else:
        print(f'There is already a case/control baseline record for {subject_id}.')



if __name__ == '__main__':
    main()
    # generate_errorlog()
    # generate_success_list()