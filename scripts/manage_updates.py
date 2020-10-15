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

publish_data = False
user_input_subject_type = 'other'

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global LOADFILE
    global publish_data
    global user_input_subject_type

    publish_data = get_publish_action()
    user_input_subject_type = get_subject_type()
    LOADFILE = get_filename()
    data_dict = create_data_dict(LOADFILE)
    write_to_db(data_dict)

def write_to_db(data_dict):
    global publish_data

    """takes data dict and publish boolean and writes to database"""
    for key, value in data_dict.items():
        # subject_id = value["subject_id"]
        subject_id = value.pop("subject_id")
        version = value["data_version"]

        value["update_baseline"] = update_baseline_check( subject_id , user_input_subject_type , value )
        value["update_latest"] = update_latest_check( subject_id, user_input_subject_type, value )
        value["update_adstatus"] = update_adstatus_check( subject_id, user_input_subject_type, value )
        value["correction"] = correction_check( value )

        _data = json.dumps(value)

        if publish_data:
            database_connection(f"UPDATE ds_subjects_phenotypes SET(subject_id, _data, published) = ('{subject_id}', '{_data}', TRUE) WHERE subject_id = '{subject_id}' AND subject_type = '{user_input_subject_type}' AND _data->>'data_version' = '{version}' AND published = FALSE")
        else:
            database_connection(f"UPDATE ds_subjects_phenotypes SET(subject_id, _data) = ('{subject_id}', '{_data}') WHERE subject_id = '{subject_id}' AND subject_type = '{user_input_subject_type}' AND _data->>'data_version' = '{version}' AND published = FALSE")

def create_data_dict(LOADFILE):
    """takes loadfile name as arg, returns dict of json data keyed by subject id of data to be entered in database"""
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
                    if check_not_duplicate( blob, "PUBLISHED = TRUE" ):
                        data_dict[f'{blob["subject_id"]}_{blob["release_version"]}'] = blob
                    else:
                        print(f'Already a published entry for {blob["subject_id"]} in {blob["release_version"]}. No update will be added to database.  Check database and loadfile')
                else:
                    print(f"Version {blob['data_version']} not found. Record will not be added. Check database.")


    for key, record in data_dict.items():
        """remove release_version from blob for each record in dict, in db is joined from data_version table"""
        record.pop('release_version')

    return data_dict


if __name__ == '__main__':
    main()
    # generate_errorlog()
    # generate_success_list()