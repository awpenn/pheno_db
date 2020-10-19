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

error_log = {}

user_input_subject_type = ''


def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global user_input_subject_type

    user_input_subject_type = get_subject_type()
    LOADFILE = get_filename()
    drop_dict = create_drop_data_dict(LOADFILE)
    drop_from_database(drop_dict)

def create_drop_data_dict(LOADFILE):
    """takes loadfile name as arg, returns dict of json data keyed by subject id of data to be entered in database"""
    global user_input_subject_type
    drop_dict = {}
    if user_input_subject_type == 'case/control':
        view_based_on_subject_type = 'get_unpublished_updates_cc'
    if user_input_subject_type == 'family':
        view_based_on_subject_type = 'get_unpublished_updates_fam'

    with open(f'./source_files/{LOADFILE}', mode='r', encoding='utf-8-sig') as csv_file:
        pheno_file = csv.reader(csv_file)
        headers = next(pheno_file)
        
        for row in pheno_file:
            if pheno_file.line_num > 1:
                for index, value in enumerate(row):
                    if headers[index].lower() == 'subject_id':
                        if check_subject_exists(view_based_on_subject_type, value, row[headers.index("release_version")]):
                            id = value
                        else:
                            print(f"No record found for {value} in given data version.  Check that id and release name are correct.")
                    elif headers[index].lower() == 'release_version':
                        release_version = get_data_version_id(value)
                    else:
                        print(f'{headers[index]} is an unknown data type.  Your load file should contain only subject_id and release_version fields.')
                        break

                try:
                    if isinstance(release_version, int):
                        drop_dict[id] = release_version
                    else:
                        print(f"release_version ({value}) given for {id} is not in database.  Subject drop discarded.  Check data.")
                except:
                    print('There is an error with your data.  Please check correctness.  Program will terminate.')
                    sys.exit()

    return drop_dict

def drop_from_database(drop_dict):
    """takes dict keyed by subject id, with id for data version in which subject's record will be deleted."""
    for key, value in drop_dict.items():
        database_connection(f"DELETE FROM ds_subjects_phenotypes WHERE subject_id = '{key}' AND _data->>'data_version' = '{value}' AND published = FALSE")

if __name__ == '__main__':
    main()
