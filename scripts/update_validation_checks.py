import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json
import datetime

import pandas as pd

from pheno_utils import *
from subject_objects import *
    
def main():
    ## dict of class names for different subject_types to pass into update_checks function
    classname_dict = { subjname: classname for ( subjname, classname ) in database_connection("SELECT subject_type, subject_classname FROM env_var_by_subject_type") }

    # user_input_subject_type = get_subject_type()

    # LOADFILE = get_filename()

    ## testing vars
    
    user_input_subject_type = 'case/control'
    LOADFILE = 'a.csv'

    dict_name = database_connection(f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ user_input_subject_type }'")[ 0 ][ 0 ]
    dictionary = get_dict_data( dict_name )

    ## data from comparison csv now in subjid-keyed json
    comparison_data = create_data_dict( LOADFILE, user_input_subject_type )
    run_update_checks( comparison_data, dictionary, classname_dict, user_input_subject_type )

def create_data_dict( LOADFILE, subject_type ):
    """takes loadfile, subject_type as args, returns dict of json data keyed by subject id of data to be valcheck"""
    release_dict = build_release_dict()
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

                data_dict[ blob["subject_id"] ] = blob

    return data_dict

def run_update_checks( comparison_data, dictionary, classname_dict, subject_type ):
    """takes the jsonified comparison_data, the appropriate dictionary, subject_type, and classname_dict..."""
    errors_dict = {}
    for key, value in comparison_data.items():
        ## uses the etattr to get the reference to class based on classname from dict, then instantiates subject object with 'value' data
        subject = getattr( sys.modules[ __name__ ], classname_dict[ subject_type ] )( value )

        subject_data_errors = subject.run_checks()

        if subject_data_errors:
            errors_dict[ key ] = subject_data_errors
            
    print( errors_dict )

if __name__ == '__main__':
    print('start ', datetime.datetime.now().strftime("%H:%M:%S") )
    main()  
    print('end ', datetime.datetime.now().strftime("%H:%M:%S") )