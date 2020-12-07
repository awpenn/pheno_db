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

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""

    user_input_subject_type = get_subject_type()
    views_based_on_subject_type = get_views_by_subject_type( user_input_subject_type )

    for view in views_based_on_subject_type:
        if 'unpublished' in view:
            view_based_on_subject_type = view

    breakpoint()
    is_batch_file = user_input_batch_loading()

    if is_batch_file:
       LOADFILE = get_filename()
       drop_dict = create_drop_data_dict( LOADFILE, view_based_on_subject_type )

    else:
       drop_dict = get_subject_to_drop( view_based_on_subject_type )
    
    # drop_dict returns dict keyed by subject id with data_version (pkey from table) as value
    drop_from_database( user_input_subject_type, drop_dict )

def create_drop_data_dict( LOADFILE, view_based_on_subject_type ):
    """takes loadfile name and view to look up based on subject_type as args, 
    returns dict of json data keyed by subject id of data to be entered in database"""
    drop_dict = {}

    with open(f'./source_files/{LOADFILE}', mode='r', encoding='utf-8-sig') as csv_file:
        pheno_file = csv.reader(csv_file)
        headers = next(pheno_file)
        
        for index, row in enumerate(pheno_file): 
            subjid = row[headers.index( "subjid" ) ] or None
            release_version = row[headers.index( "release_version" ) ] or None
            data_version_id = get_data_version_id( release_version ) or None
            
            missing_data = True in ( ele == None for ele in [ subjid, release_version, data_version_id ] )

            if missing_data:
                print(f'There are empty fields and/or data errors in the the loadfile, row { index + 1 }')
                continue
            else:
                if pheno_file.line_num > 1:
                    if check_subject_exists( view_based_on_subject_type, subjid, release_version ):
                        try:
                            if isinstance(data_version_id, int):
                                drop_dict[subjid] = data_version_id
                        except:
                            print(f"release_version ({release_version}) given for {subjid} is not in database.  Subject drop discarded.  Check data.")
                    else:
                        print(f'{subjid} has no unpublished record in {row[headers.index("release_version")]}.  Subject drop discarded. Check data.')
              
    return drop_dict

def drop_from_database( subject_type, drop_dict ):
    """takes subject_type and dict keyed by subject id, with id for data version in which subject's record will be deleted."""
    for key, value in drop_dict.items():
        database_connection(f"DELETE FROM ds_subjects_phenotypes WHERE subject_id = '{ key }' AND _data->>'data_version' = '{ value }' AND subject_type = '{ subject_type }' AND published = FALSE")

if __name__ == '__main__':
    main()
