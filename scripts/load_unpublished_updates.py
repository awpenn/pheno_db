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

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global user_input_subject_type
    
    user_input_subject_type = get_subject_type()
    LOADFILE = get_filename()
    data_dict = create_data_dict(LOADFILE)
    write_to_db(data_dict)

def write_to_db(data_dict):
    """takes data dict and writes to database"""

    global user_input_subject_type
    for key, value in data_dict.items():
        subject_id = value["subject_id"]
        value.pop("subject_id")

        value["update_baseline"] = update_baseline_check( subject_id , user_input_subject_type , value )
        value["update_latest"] = update_latest_check( subject_id, user_input_subject_type, value )
        value["update_adstatus"] = update_adstatus_check( subject_id, user_input_subject_type, value )
        try:
            value["correction"]
        except:
            value["correction"] = 0

        _data = json.dumps(value)
        database_connection(f"INSERT INTO ds_subjects_phenotypes(subject_id, _data, subject_type) VALUES('{subject_id}', '{_data}', '{user_input_subject_type}')")
        
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
                    if headers[index].lower() == 'latest_update_release_version':
                        blob["latest_update_version"] = get_data_version_id(value)
                    

                if type(blob["data_version"]) == int:
                    if check_not_duplicate( blob, 'PUBLISHED = FALSE' ) and check_not_duplicate( blob, 'PUBLISHED = TRUE' ):
                        data_dict[f'{blob["subject_id"]}_{blob["release_version"]}'] = blob
                    else:
                        print(f'Already an update or published entry for {blob["subject_id"]} in {blob["release_version"]}. No update will be added to database.  Check database and loadfile')
                else:
                    print(f"Version {blob['data_version']} not found. Record will not be added. Check database.")


    for key, record in data_dict.items():
        """remove release version and latest update version, 
        because these are imported as strings, but stored by pkey and 
        joined in view"""
        record.pop('release_version')
        record.pop('latest_update_release_version')

    return data_dict

if __name__ == '__main__':
    main()
    # generate_errorlog()
    # generate_success_list()